use structopt::StructOpt;
use kvs::KvStore;
use std::io::prelude::*;
use std::net::TcpStream;
use protocol::{Package, construct_package, deconstruct_package};
mod protocol;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short, long = "addr")]
    address: String,
    #[structopt(subcommand)]
    command: Command
}

#[derive(Debug, StructOpt)]
enum Command {
    #[structopt(name="get")]
    Get {
        key: String
    },
    #[structopt(name="set")]
    Set {
        key: String,
        val: String
    },
    #[structopt(name="rm")]
    Remove {
        key: String
    },
}

fn main() -> std::io::Result<()> {
    let opt = Opt::from_args();

    let mut storage = kvs::KvStore::open(".").expect("cannot open kvs storage") ;
    match opt.command {
    Command::Get {key} => {
            match storage.get(key) {
                Ok(Some(val)) => println!("{}", val),
                Ok(None) => println!("Key not found"),
                Err(e) => println!("{}", e),
            };
    },
    Command::Set {key, val} => storage.set(key, val).expect("unexpected error in setting process"),
    Command::Remove {key} => storage.remove(key).expect("Key not found"),
    }

    let mut socket = TcpStream::connect(opt.address.clone())?;
    socket.write(&construct_package(Package::Get("maxim, that is me, gonna be asleep".as_bytes())))?;
    drop(socket);
    let mut socket = TcpStream::connect(opt.address)?;
    socket.write(&construct_package(Package::Set("have a greate night, bye".as_bytes(), "!".as_bytes())))?;

    Ok(())
}
