#[tokio::main]
async fn main() {
    libzoa::server::do_server().await;
}
