use log::*;

fn setup_logging() {
    let base_config = fern::Dispatch::new()
        .level(LevelFilter::Trace)
        .level_for("rusoto_core::request", LevelFilter::Info)
        .level_for("want", LevelFilter::Debug);

    let file_config = fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}][{}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.6f"),
                record.target(),
                record.level(),
                message
            ))
        })
        .chain(fern::log_file("zoa.log").unwrap());

    base_config.chain(file_config).apply().unwrap();
}

fn main() {
    setup_logging();

    warn!(
        "local timezone is {}",
        chrono::Local::now().format("%Z (%:z)")
    );

    let socket_path = std::env::args()
        .nth(2)
        .unwrap_or("/run/zfs_{}_socket".to_string());
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("zoa")
        .build()
        .unwrap()
        .block_on(async move {
            libzoa::server::do_server(&socket_path).await;
        });
}
