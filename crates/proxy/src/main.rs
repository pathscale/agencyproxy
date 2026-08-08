use agency_proxy::ProxyServer;
use clap::Parser;
use std::{path::PathBuf, process::ExitCode};

#[derive(Debug, Parser)]
#[command(name = "agency-proxy", version, about)]
struct Args {
    /// Permission-restricted local endpoint used by AgencyZero clients.
    #[arg(long, value_name = "PATH")]
    socket: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    let socket_path = Args::parse().socket;
    let server = match ProxyServer::bind(&socket_path).await {
        Ok(server) => server,
        Err(error) => {
            eprintln!("could not start AgencyProxy: {error}");
            return ExitCode::FAILURE;
        }
    };
    eprintln!(
        "AgencyProxy listening on {}",
        server.socket_path().display()
    );
    match server.serve().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("AgencyProxy stopped: {error}");
            ExitCode::FAILURE
        }
    }
}
