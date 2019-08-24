extern crate clap;
use clap::{App, Arg, SubCommand};

extern crate kvs;
use kvs::KvStore;

fn main() {
    let matches = App::new("kvs")
        .subcommand(SubCommand::with_name("set").args(&[
            Arg::with_name("key").required(true).index(1),
            Arg::with_name("value").required(true).index(2),
        ]))
        .subcommand(
            SubCommand::with_name("get").args(&[Arg::with_name("key").required(true).index(1)]),
        )
        .subcommand(
            SubCommand::with_name("rm").args(&[Arg::with_name("key").required(true).index(1)]),
        )
        .arg(
            Arg::with_name("V")
                .short("V")
                .help("Print the version of crate"),
        )
        .get_matches();

    if matches.occurrences_of("V") > 0 {
        println!(env!("CARGO_PKG_VERSION"));
        return;
    }

    match matches.subcommand() {
        ("set", Some(m)) => KvStore::open(std::path::Path::new(&std::env::current_dir().unwrap()))
            .unwrap()
            .set(
                m.value_of("key").unwrap().to_owned(),
                m.value_of("value").unwrap().to_owned(),
            )
            .unwrap(),
        ("get", Some(m)) => {
            let value = KvStore::open(std::path::Path::new(&std::env::current_dir().unwrap()))
                .unwrap()
                .get(m.value_of("key").unwrap().to_owned());

            match value {
                Ok(Some(val)) => println!("{}", val),
                Ok(None) => println!("Key not found"),
                Err(e) => println!("{}", e),
            };
        }

        ("rm", Some(m)) => {
            let value = KvStore::open(std::path::Path::new(&std::env::current_dir().unwrap()))
                .unwrap()
                .remove(m.value_of("key").unwrap().to_owned());

            match value {
                Err(e)=> 
                {
                    println!("Key not found");
                    std::process::exit(1);
                },
                _ => (),
            };
        }
        _ => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
    }
}
