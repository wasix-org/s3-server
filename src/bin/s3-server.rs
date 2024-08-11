//! ```shell
//! s3-server 0.2.0-dev
//!
//! USAGE:
//!     s3-server [OPTIONS]
//!
//! FLAGS:
//!     -h, --help       Prints help information
//!     -V, --version    Prints version information
//!
//! OPTIONS:
//!         --fs-root <fs-root>           [default: .]
//!         --host <host>                 [default: localhost]
//!         --port <port>                 [default: 8080]
//!         --access-key <access-key>
//!         --secret-key <secret-key>
//! ```

#![forbid(unsafe_code)]

use s3_server::storages::fs::FileSystem;
use s3_server::S3Service;
use s3_server::SimpleAuth;
use tracing::trace;

use std::net::TcpListener;
use std::path::PathBuf;

use anyhow::Result;
use futures::future;
use hyper::server::Server;
use hyper::service::make_service_fn;
use structopt::StructOpt;
use structopt_flags::LogLevel;
use tracing::{debug, info};

#[derive(StructOpt)]
struct Args {
    #[structopt(long, default_value = ".", env = "FS_ROOT")]
    fs_root: PathBuf,

    #[structopt(long, default_value = "localhost", env = "HOST")]
    host: String,

    #[structopt(long, default_value = "8080", env = "PORT")]
    port: u16,

    #[structopt(long, requires("secret-key"), display_order = 1000, env = "ACCESS_KEY")]
    access_key: Option<String>,

    #[structopt(long, requires("access-key"), display_order = 1000, env = "SECRET_KEY")]
    secret_key: Option<String>,

    #[structopt(flatten)]
    verbose: structopt_flags::QuietVerbose,

    #[structopt(long)]
    allow_bucket_manipulation: bool,
}

fn setup_tracing(args: &Args) {
    use tracing_error::ErrorLayer;
    use tracing_subscriber::fmt::time::UtcTime;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{fmt, EnvFilter};

    let filter = args.verbose.get_level_filter();

    tracing_subscriber::fmt()
        .event_format(fmt::format::Format::default().compact())
        .with_env_filter(EnvFilter::from_default_env())
        .with_max_level(match filter {
            structopt_flags::LevelFilter::Off => tracing::Level::ERROR,
            structopt_flags::LevelFilter::Error => tracing::Level::WARN,
            structopt_flags::LevelFilter::Warn => tracing::Level::INFO,
            structopt_flags::LevelFilter::Info => tracing::Level::DEBUG,
            structopt_flags::LevelFilter::Debug => tracing::Level::TRACE,
            structopt_flags::LevelFilter::Trace => tracing::Level::TRACE,
        })
        .with_timer(UtcTime::rfc_3339())
        .finish()
        .with(ErrorLayer::default())
        .init();

    trace!(?filter);
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let args: Args = Args::from_args();
    setup_tracing(&args);

    // setup the storage
    trace!("Initializing file system ({:?})", args.fs_root);
    let fs = FileSystem::new(&args.fs_root)?;
    debug!(?fs);

    // setup the service
    trace!("S3 service new");
    let mut service = S3Service::new(fs);

    if let (Some(access_key), Some(secret_key)) = (args.access_key, args.secret_key) {
        trace!("Loading access key and secret");

        let mut auth = SimpleAuth::new();
        auth.register(access_key, secret_key);
        debug!(?auth);
        service.set_auth(auth);
    }

    s3_server::DISALLOW_BUCKET_MANIPULATION.store(
        !args.allow_bucket_manipulation,
        std::sync::atomic::Ordering::Relaxed,
    );

    let server = {
        let service = service.into_shared();

        trace!(
            "Binding TCP on ({}) with port {}",
            args.host.as_str(),
            args.port
        );
        let listener = TcpListener::bind((args.host.as_str(), args.port))?;

        trace!("Starting S3 service on socket");
        let make_service: _ =
            make_service_fn(move |_| future::ready(Ok::<_, anyhow::Error>(service.clone())));
        Server::from_tcp(listener)?.serve(make_service)
    };

    info!("server is running at http://{}:{}/", args.host, args.port);
    server.await?;

    Ok(())
}
