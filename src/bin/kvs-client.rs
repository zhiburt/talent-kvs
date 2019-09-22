use structopt::StructOpt;
use kvs::{
    Package, 
    ok_package,
    construct_package,
    deconstruct_package
};
use std::io::prelude::*;
use std::net::TcpStream;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(subcommand)]
    command: Command
}

#[derive(Debug, StructOpt)]
enum Command {
    #[structopt(name="get")]
    Get {
        key: String,
        #[structopt(short, long = "addr")]
        addr: String,
    },
    #[structopt(name="set")]
    Set {
        key: String,
        val: String,
        #[structopt(short, long = "addr")]
        addr: String,
    },
    #[structopt(name="rm")]
    Remove {
        key: String,
        #[structopt(short, long = "addr")]
        addr: String,
    },
}

fn main() -> std::io::Result<()> {
    let opt = Opt::from_args();

    let mut buffer = [0; 1024];
    match opt.command {
    Command::Get {key, addr} => {
        let mut socket = TcpStream::connect(addr.clone())?;
        socket.write(&construct_package(Package::Get(key.as_bytes())))?;
        socket.read(&mut buffer)?;
        match deconstruct_package(&buffer) {
            Package::OK(val) => {
                if val.len() > 0 {
                    // this is a problem should be refactored
                    println!("{}", std::str::from_utf8(val).unwrap().trim_matches(char::from(0)));
                } else {
                    println!("Key not found");
                }
            },
            Package::Error(e) => println!("{}", std::str::from_utf8(e).unwrap()),
            _ => unreachable!(),
        };
    },
    Command::Set {key, val, addr} => {
            let mut socket = TcpStream::connect(addr.clone())?;
            socket.write(&construct_package(Package::Set(key.as_bytes(), val.as_bytes())))?;
            socket.read(&mut buffer)?;
            match deconstruct_package(&buffer) {
                Package::OK(val) => (),
                Package::Error(e) => println!("{}", std::str::from_utf8(e).unwrap()),
                _ => unreachable!(),
            };
    },
    Command::Remove {key, addr} => {
        let mut socket = TcpStream::connect(addr.clone())?;
        socket.write(&construct_package(Package::Remove(key.as_bytes())))?;
        socket.read(&mut buffer)?;
        match deconstruct_package(&buffer) {
            Package::OK(_) => (),
            Package::Error(e) => {
                eprintln!("{}", std::str::from_utf8(e).unwrap().trim_matches(char::from(0)));
                std::process::exit(1);
            },
            _ => unreachable!(),
        };
    },
    _ => unreachable!(),
    };

    Ok(())
}
