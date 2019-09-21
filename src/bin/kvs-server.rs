use structopt::StructOpt;
use log::{info};
use std::net::{TcpListener, TcpStream};
use std::io::{
    Result,
    prelude::*,
};
mod protocol;
use protocol::{Package, construct_package, deconstruct_package};

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short, long = "addr")]
    address: String,
    #[structopt(short = "e", long = "engine")]
    engine: String,
}

fn main() -> Result<()> {
    stderrlog::new().verbosity(10).quiet(false).init().unwrap();
    let opt = Opt::from_args();

    info!("{} version={}, config={:?}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION_PRE"), opt);
    let listener = TcpListener::bind(opt.address)?;
   for stream in listener.incoming() {
        let mut conn = stream?;
        info!("got connection to socket {}",  conn.peer_addr()?);
        let mut buffer = [0; 128];
        let s = conn.read(&mut buffer)?;

        println!("I got {}", deconstruct_package(&buffer));
    }

    Ok(())
}

// read package
// send ok
// send responce
fn handle (socket: TcpStream) -> std::io::Result<()> {
    Ok(()) 
}
