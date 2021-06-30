use futures::Future;
use log::*;
use nvpair::{NvEncoding, NvList};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixStream;
use tokio::sync::Mutex;

pub struct Connection<S> {
    state: S,
    timeout: Option<(Duration, Box<dyn TimeoutHandler<S>>)>,
    handlers: HashMap<String, HandlerEnum<S>>,
    input: OwnedReadHalf,
    output: Arc<Mutex<OwnedWriteHalf>>,
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

pub trait SerialHandler<S>: Send + Sync + 'static {
    fn invoke(
        &self,
        state: S,
        request: NvList,
    ) -> Pin<Box<dyn Future<Output = (S, Option<NvList>)> + Send>>;
}

pub trait TimeoutHandler<S>: Send + Sync + 'static {
    fn invoke(&self, state: &mut S);
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
        Box::pin((self)(state, request))
    }
}

impl<F: Send + Sync + 'static, S: Send> TimeoutHandler<S> for F
where
    F: Fn(&mut S),
{
    fn invoke(&self, state: &mut S) {
        (self)(state)
    }
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
        Box::pin((self)(state, request))
    }
}

impl<S: Send + 'static> Connection<S> {
    pub fn new(stream: UnixStream, state: S) -> Connection<S> {
        let (input, output) = stream.into_split();
        Connection {
            input,
            output: Arc::new(Mutex::new(output)),
            state,
            timeout: None,
            handlers: Default::default(),
        }
    }

    /// Regular, concurrent operations.  The server runs the handler's future in a new task.
    pub fn register_handler(&mut self, request_type: &str, handler: Box<dyn Handler<S>>) {
        self.handlers
            .insert(request_type.to_owned(), HandlerEnum::Concurrent(handler));
    }

    pub fn register_timeout_handler(
        &mut self,
        timeout: Duration,
        handler: Box<dyn TimeoutHandler<S>>,
    ) {
        self.timeout = Some((timeout, handler));
    }

    /// For serial operations.  The server awaits for the serial handler's
    /// future to complete before processing the next operation.
    pub fn register_serial_handler(
        &mut self,
        request_type: &str,
        handler: Box<dyn SerialHandler<S>>,
    ) {
        self.handlers
            .insert(request_type.to_owned(), HandlerEnum::Serial(handler));
    }

    async fn get_next_request(&mut self) -> tokio::io::Result<NvList> {
        // XXX kernel sends this as host byte order
        let len64 = self.input.read_u64_le().await?;
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
        self.input.read_exact(v.as_mut()).await?;
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

    pub fn start(mut self) {
        tokio::spawn(async move {
            loop {
                /*
                let nvl = match self.get_next_request().await {
                    Err(e) => {
                        info!("got error reading from connection: {:?}", e);
                        return;
                    }
                    Ok(nvl) => nvl,
                };
                */
                let result = match &self.timeout {
                    Some((duration, _handler)) => {
                        match tokio::time::timeout(*duration, self.get_next_request()).await {
                            Ok(result) => result,
                            Err(_) => {
                                // timed out.
                                // ugh so icky but we can't hold a ref to the handler across self.get_next_request()
                                self.timeout.as_ref().unwrap().1.invoke(&mut self.state);
                                //handler.invoke(&mut self.state);
                                continue;
                            }
                        }
                    }
                    None => self.get_next_request().await,
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
                match self.handlers.get(request_type) {
                    Some(HandlerEnum::Serial(handler)) => {
                        let fut = handler.invoke(self.state, nvl);
                        let (state, response_opt) = fut.await;
                        if let Some(response) = response_opt {
                            Self::send_response(&self.output, response).await;
                        }
                        self.state = state;
                    }
                    Some(HandlerEnum::Concurrent(handler)) => {
                        let fut = handler.invoke(&mut self.state, nvl);
                        let output = self.output.clone();
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
