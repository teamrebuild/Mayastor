use mbus_api::{v0::*, *};
use structopt::StructOpt;
use tracing::info;

#[derive(Debug, StructOpt)]
struct CliArgs {
    /// The Nats Server URL to connect to
    /// (supports the nats schema)
    /// Default: nats://127.0.0.1:4222
    #[structopt(long, short, default_value = "nats://127.0.0.1:4222")]
    url: String,
}

fn init_tracing() {
    if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter("info").init();
    }
}

#[tokio::main]
async fn main() {
    init_tracing();

    client().await;
}

async fn client() {
    let cli_args = CliArgs::from_args();
    mbus_api::message_bus_init(cli_args.url).await;

    ConfigUpdate {
        kind: Config::MayastorConfig,
        data: "My config...".into(),
    }
    .request()
    .await
    .unwrap();

    let config = GetConfig::Request(
        &ConfigGetCurrent {
            kind: Config::MayastorConfig,
        },
        Channel::v0(v0::ChannelVs::Kiiss),
        bus(),
    )
    .await
    .unwrap();

    info!(
        "Received config: {:?}",
        std::str::from_utf8(&config.config).unwrap()
    );
}
