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

    let mut socket = TcpStream::connect(opt.address.clone())?;
    let mut buffer = [0; 1024];
    match opt.command {
    Command::Get {key} => {
        socket.write(&construct_package(Package::Get(key.as_bytes())))?;
        socket.read(&mut buffer)?;
        match deconstruct_package(&buffer) {
            Package::OK(val) => {
                if val.len() > 0 {
                    println!("{}", std::str::from_utf8(val).unwrap())
                } else {
                    println!("Key not found")
                }
            },
            Package::Error(e) => println!("{}", std::str::from_utf8(e).unwrap()),
            _ => unreachable!(),
        };
    },
    Command::Set {key, val} => {
            socket.write(&construct_package(Package::Set(key.as_bytes(), val.as_bytes())))?;
            socket.read(&mut buffer)?;
            match deconstruct_package(&buffer) {
                Package::OK(val) => (),
                Package::Error(e) => println!("{}", std::str::from_utf8(e).unwrap()),
                _ => unreachable!(),
            };
    },
    Command::Remove {key} => {
        socket.write(&construct_package(Package::Remove(key.as_bytes())))?;
        socket.read(&mut buffer)?;
        match deconstruct_package(&buffer) {
            Package::OK(val) => (),
            Package::Error(e) => println!("{}", std::str::from_utf8(e).unwrap()),
            _ => unreachable!(),
        };
    },
    _ => unreachable!(),
    };

    Ok(())
}
