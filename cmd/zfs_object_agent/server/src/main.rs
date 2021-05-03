fn main() {
    let socket_path = std::env::args()
        .nth(2)
        .unwrap_or("/run/zfs_socket".to_string());
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("zoa")
        .build()
        .unwrap()
        .block_on(async move {
            libzoa::server::do_server(&socket_path).await;
        });
}
