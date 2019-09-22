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
    SledStorage,
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

    if let Some(old_engine) = current_engine(std::env::current_dir()?){
        if old_engine != opt.engine {
            eprintln!("the storage was configured already using another engine");
            std::process::exit(1);
        }
    } else {
        pin_engine(std::env::current_dir()?, opt.engine.clone())?;
    }

    let addr = opt.address.parse::<std::net::SocketAddr>().expect("cannot parse socket address");
    if opt.engine == "kvs" {
        run(KvStore::open(std::env::current_dir()?).expect("cannot open kvs store"), addr)?;
    } else if opt.engine == "sled" {
        run(SledStorage::open(std::env::current_dir()?).expect("cannot open sled storage"), addr)?;
    } else {
        error!("wrong engine");
        std::process::exit(1);
    };

    Ok(())
}

fn pin_engine(path: std::path::PathBuf, engine_name: String) -> Result<()> {
    let mut f = std::fs::File::create(path.join("engine"))?;
    f.write(engine_name.as_ref())?;
    f.flush()
}

fn current_engine(path: std::path::PathBuf) -> Option<String> {
    let p = path.join("engine");
    let engine_name = std::fs::read(p);
    if engine_name.is_err() {
        return None;
    }

    Some(String::from_utf8(engine_name.unwrap()).unwrap())
}

fn run<E: KvsEngine>(mut engine: E, addr: std::net::SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr)?;
    for stream in listener.incoming() {
        let mut conn = stream?;
        info!("got connection to socket {}",  conn.peer_addr()?);
        
        handle(conn, &mut engine)?;
    };

    Ok(())
}

// read package
// send ok
// send responce
fn handle<E: KvsEngine>(mut socket: TcpStream, kvs: &mut E) -> std::io::Result<()> {
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
                    socket.write(&construct_package(Package::Error("Key not found".as_bytes())))?;
                    warn!("send error");
                };
            },
        Package::Get(key) => {
            match kvs.get(std::str::from_utf8(key).unwrap().to_owned()) {
                Ok(Some(val)) => {
                    // what is happening with size of the value?
                    socket.write(&construct_package(Package::OK(val.trim_matches(char::from(0)).as_ref())))?;
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
