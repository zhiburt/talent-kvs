use structopt::StructOpt;
use log::{info, warn, error};
use std::net::{TcpListener, TcpStream};
use std::io::{
    Result,
    prelude::*,
};
use kvs::{
    KvStore,
    KvsEngine,
    Package, 
    ok_package,
    construct_package,
    deconstruct_package
};

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short, long = "addr")]
    address: String,
    #[structopt(short = "e", long = "engine")]
    engine: String,
}

fn main() -> Result<()> {
    stderrlog::new().module(module_path!()).init().unwrap();
    let opt = Opt::from_args();

    error!("{} version={}, address={}, engine={}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"), opt.address, opt.engine);

    let mut kvs = KvStore::open(".").expect("cannot open kvs store");
    
    let listener = TcpListener::bind(opt.address)?;
    for stream in listener.incoming() {
        let mut conn = stream?;
        info!("got connection to socket {}",  conn.peer_addr()?);
        
        handle(conn, &mut kvs)?;
    }

    Ok(())
}

// read package
// send ok
// send responce
fn handle (mut socket: TcpStream, kvs: &mut kvs::KvStore) -> std::io::Result<()> {
    let mut buffer = [0; 1024];
    socket.read(&mut buffer)?;
    let pkg = deconstruct_package(&buffer);
    info!("I got {}", pkg);

    match pkg {
        Package::Remove(key) => {
            if kvs.remove(std::str::from_utf8(key).unwrap().to_owned()).is_ok() {
                socket.write(&construct_package(ok_package()))?;
                info!("send blank OK");
            } else {
                    socket.write(&construct_package(Package::Error("cannot be found key".as_bytes())))?;
                    warn!("send error");
                };
            },
        Package::Get(key) => {
            match kvs.get(std::str::from_utf8(key).unwrap().to_owned()) {
                Ok(Some(val)) => {
                    // what is happening with size of the value?
                    socket.write(&construct_package(Package::OK(val.as_ref())))?;
                    info!("send OK {}", val);
                },
                Ok(None) => {
                    socket.write(&construct_package(ok_package()))?;
                    info!("send ok with none");
                },
                Err(msg) => {
                    socket.write(&construct_package(Package::Error("error happend".as_bytes())))?;
                    info!("send error");
                }
            }
        },
        Package::Set(key, val) => {
            if kvs.set(std::str::from_utf8(key).unwrap().to_owned(), std::str::from_utf8(val).unwrap().to_owned()).is_ok() {
                socket.write(&construct_package(ok_package()))?;
                info!("send blank OK");
            } else {
                    socket.write(&construct_package(Package::Error("something went wrong".as_bytes())))?;
                    warn!("send error");
                };
        },
        _ => unreachable!(),
        };

    Ok(()) 
}
