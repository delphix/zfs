use futures::Future;
use log::*;
use nvpair::{NvEncoding, NvList};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

pub struct Server<Ss, Cs> {
    socket_path: String,
    state: Ss,
    connection_handler: Box<ConnectionHandler<Ss, Cs>>,
    timeout: Option<(Duration, Box<TimeoutHandler<Cs>>)>,
    handlers: HashMap<String, HandlerEnum<Cs>>,
}

enum HandlerEnum<S> {
    Serial(Box<dyn SerialHandler<S>>),
    Concurrent(Box<dyn Handler<S>>),
}

pub trait Handler<S>: Send + Sync + 'static {
    fn invoke(
        &self,
        state: &mut S,
        request: NvList,
    ) -> Pin<Box<dyn Future<Output = Option<NvList>> + Send>>;
}

impl<F: Send + Sync + 'static, Fut, S: Send> Handler<S> for F
where
    F: Fn(&mut S, NvList) -> Fut,
    Fut: Future<Output = Option<NvList>> + Send + 'static,
{
    fn invoke(
        &self,
        state: &mut S,
        request: NvList,
    ) -> Pin<Box<dyn Future<Output = Option<NvList>> + Send + 'static>> {
        Box::pin(self(state, request))
    }
}

pub trait SerialHandler<S>: Send + Sync + 'static {
    fn invoke(
        &self,
        state: S,
        request: NvList,
    ) -> Pin<Box<dyn Future<Output = (S, Option<NvList>)> + Send>>;
}

impl<F: Send + Sync + 'static, Fut, S: Send> SerialHandler<S> for F
where
    F: Fn(S, NvList) -> Fut,
    Fut: Future<Output = (S, Option<NvList>)> + Send + 'static,
{
    fn invoke(
        &self,
        state: S,
        request: NvList,
    ) -> Pin<Box<(dyn Future<Output = (S, Option<NvList>)> + Send + 'static)>> {
        Box::pin(self(state, request))
    }
}

/*
pub trait TimeoutHandler<S>: Send + Sync + 'static {
    fn invoke(&self, state: &mut S);
}

impl<F: Send + Sync + 'static, S: Send> TimeoutHandler<S> for F
where
    F: Fn(&mut S),
{
    fn invoke(&self, state: &mut S) {
        self(state)
    }
}
*/

type TimeoutHandler<S> = dyn Fn(&mut S) + Send + Sync + 'static;
type ConnectionHandler<Svr, State> = dyn Fn(&Svr) -> State + Send + Sync + 'static;

impl<Ss: Send + Sync + 'static, Cs: Send + Sync + 'static> Server<Ss, Cs> {
    pub fn new(
        socket_path: &str,
        state: Ss,
        connection_handler: Box<ConnectionHandler<Ss, Cs>>,
    ) -> Server<Ss, Cs> {
        Server {
            socket_path: socket_path.to_owned(),
            state,
            connection_handler,
            timeout: None,
            handlers: Default::default(),
        }
    }

    /// Regular, concurrent operations.  The server runs the handler's future in a new task.
    pub fn register_handler(&mut self, request_type: &str, handler: Box<dyn Handler<Cs>>) {
        self.handlers
            .insert(request_type.to_owned(), HandlerEnum::Concurrent(handler));
    }

    pub fn register_timeout_handler(
        &mut self,
        timeout: Duration,
        handler: Box<TimeoutHandler<Cs>>,
    ) {
        self.timeout = Some((timeout, handler));
    }

    /// For serial operations.  The server awaits for the serial handler's
    /// future to complete before processing the next operation.
    pub fn register_serial_handler(
        &mut self,
        request_type: &str,
        handler: Box<dyn SerialHandler<Cs>>,
    ) {
        self.handlers
            .insert(request_type.to_owned(), HandlerEnum::Serial(handler));
    }

    pub fn start(self) {
        let arc = Arc::new(self);
        info!("Listening on: {}", arc.socket_path);
        let _ = std::fs::remove_file(&arc.socket_path);
        let listener = UnixListener::bind(&arc.socket_path).unwrap();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        info!("accepted connection on {}", arc.socket_path);
                        let connection_state = (arc.connection_handler)(&arc.state);
                        Self::start_connection(arc.clone(), stream, connection_state);
                    }
                    Err(e) => {
                        warn!("accept() on {} failed: {}", arc.socket_path, e);
                    }
                }
            }
        });
    }

    async fn get_next_request(input: &mut OwnedReadHalf) -> tokio::io::Result<NvList> {
        // XXX kernel sends this as host byte order
        let len64 = input.read_u64_le().await?;
        //println!("got request len: {}", len64);
        if len64 > 20_000_000 {
            // max zfs block size is 16MB
            panic!("got unreasonable request length {} ({:#x})", len64, len64);
        }

        let mut v = Vec::new();
        // XXX would be nice if we didn't have to zero it out.  Should be able
        // to do that using read_buf(), treating the Vec as a BufMut, but will
        // require multiple calls to do the equivalent of read_exact().
        v.resize(len64 as usize, 0);
        input.read_exact(v.as_mut()).await?;
        let nvl = NvList::try_unpack(v.as_ref()).unwrap();
        Ok(nvl)
    }

    async fn send_response(output: &Mutex<OwnedWriteHalf>, nvl: NvList) {
        //println!("sending response: {:?}", nvl);
        let buf = nvl.pack(NvEncoding::Native).unwrap();
        drop(nvl);
        let len64 = buf.len() as u64;
        let mut w = output.lock().await;
        // XXX kernel expects this as host byte order
        //println!("sending response of {} bytes", len64);
        w.write_u64_le(len64).await.unwrap();
        w.write_all(buf.as_slice()).await.unwrap();
    }

    fn start_connection(server: Arc<Server<Ss, Cs>>, stream: UnixStream, mut state: Cs) {
        let (mut input, output) = stream.into_split();
        let output = Arc::new(Mutex::new(output));

        tokio::spawn(async move {
            loop {
                /*
                let nvl = match conn.get_next_request().await {
                    Err(e) => {
                        info!("got error reading from connection: {:?}", e);
                        return;
                    }
                    Ok(nvl) => nvl,
                };
                */
                let result = match &server.timeout {
                    Some((duration, _handler)) => {
                        match tokio::time::timeout(*duration, Self::get_next_request(&mut input))
                            .await
                        {
                            Ok(result) => result,
                            Err(_) => {
                                // timed out.
                                // ugh so icky but we can't hold a ref to the handler across conn.get_next_request()
                                server.timeout.as_ref().unwrap().1(&mut state);
                                //handler.invoke(&mut conn.state);
                                continue;
                            }
                        }
                    }
                    None => Self::get_next_request(&mut input).await,
                };
                let nvl = match result {
                    Ok(nvl) => nvl,
                    Err(e) => {
                        info!("got error reading from connection: {:?}", e);
                        return;
                    }
                };

                let request_type_cstr = nvl.lookup_string("Type").unwrap();
                let request_type = request_type_cstr.to_str().unwrap();
                match server.handlers.get(request_type) {
                    Some(HandlerEnum::Serial(handler)) => {
                        let (new_state, response_opt) = handler.invoke(state, nvl).await;
                        if let Some(response) = response_opt {
                            Self::send_response(&output, response).await;
                        }
                        state = new_state;
                    }
                    Some(HandlerEnum::Concurrent(handler)) => {
                        let fut = handler.invoke(&mut state, nvl);
                        let output = output.clone();
                        tokio::spawn(async move {
                            if let Some(response) = fut.await {
                                Self::send_response(&output, response).await;
                            }
                        });
                    }
                    None => {
                        panic!("bad type {:?} in request {:?}", request_type, nvl);
                    }
                }
            }
        });
    }
}
