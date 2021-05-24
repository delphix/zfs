use clap::Arg;
use log::*;

fn setup_logging(verbosity: u64, file_name: Option<&str>) {
    let mut base_config = fern::Dispatch::new();

    base_config = match verbosity {
        0 => base_config.level(LevelFilter::Warn),
        1 => base_config
            .level(LevelFilter::Info)
            .level_for("rusoto_core::request", LevelFilter::Info)
            .level_for("want", LevelFilter::Debug),
        2 => base_config
            .level(LevelFilter::Debug)
            .level_for("rusoto_core::request", LevelFilter::Info)
            .level_for("want", LevelFilter::Debug),
        3 => base_config
            .level(LevelFilter::Trace)
            .level_for("rusoto_core::request", LevelFilter::Info)
            .level_for("want", LevelFilter::Debug),
        _ => base_config.level(LevelFilter::Trace),
    };

    let mut config = fern::Dispatch::new().format(|out, message, record| {
        let target = record.target();
        let stripped_target = target.strip_prefix("libzoa").unwrap_or(target);
        out.finish(format_args!(
            "[{}][{}][{}] {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
            stripped_target,
            record.level(),
            message
        ))
    });
    config = match file_name {
        None => config.chain(std::io::stdout()),
        Some(file_name) => config.chain(fern::log_file(file_name).unwrap()),
    };

    base_config.chain(config).apply().unwrap();
}

fn main() {
    let matches = clap::App::new("ZFS Object Agent")
        .about("Enables the ZFS kernel module talk to S3-protocol object storage")
        .arg(
            Arg::with_name("verbosity")
                .short("v")
                .multiple(true)
                .help("Sets the level of logging verbosity"),
        )
        .arg(
            Arg::with_name("socket-dir")
                .short("d")
                .long("socket-dir")
                .value_name("DIR")
                .help("Directory for unix-domain sockets")
                .takes_value(true)
                .default_value("/run"),
        )
        .arg(
            Arg::with_name("output-file")
                .short("o")
                .long("output-file")
                .value_name("FILE")
                .help("File to log output to")
                .takes_value(true),
        )
        .get_matches();

    let socket_dir = matches.value_of("socket-dir").unwrap();

    setup_logging(
        matches.occurrences_of("verbosity"),
        matches.value_of("output-file"),
    );

    error!(
        "Starting ZFS Object Agent.  Local timezone is {}",
        chrono::Local::now().format("%Z (%:z)")
    );

    // error!() should be used when an invalid state is encountered; the related
    // operation will fail and the program may exit.  E.g. an invalid request
    // was received from the client (kernel).
    error!("logging level ERROR enabled");

    // warn!() should be used when something unexpected has happened, but it can
    // be recovered from.
    warn!("logging level WARN enabled");

    // info!() should be used for very high level operations which are expected
    // to happen infrequently (no more than once per minute in typical
    // operation).  e.g. opening/closing a pool, long-lived background tasks,
    // things that might be in `zpool history -i`.
    info!("logging level INFO enabled");

    // debug!() can be used for all but the most frequent operations.
    // e.g. not every single read/write/free operation, but perhaps for every
    // call to S3.
    debug!("logging level DEBUG enabled");

    // trace!() can be used indiscriminately.
    trace!("logging level TRACE enabled");

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("zoa")
        .build()
        .unwrap()
        .block_on(async move {
            libzoa::server::do_server(socket_dir).await;
        });
}
