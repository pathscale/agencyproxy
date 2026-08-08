use agency_proxy::ProxyServer;
use std::{path::PathBuf, process::ExitCode};

const USAGE: &str = "usage: agency-proxy --socket PATH";

fn socket_arg() -> Result<PathBuf, String> {
    let mut args = std::env::args_os().skip(1);
    match (args.next(), args.next(), args.next()) {
        (Some(flag), Some(path), None) if flag == "--socket" => Ok(path.into()),
        _ => Err(USAGE.into()),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let socket_path = match socket_arg() {
        Ok(path) => path,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
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
