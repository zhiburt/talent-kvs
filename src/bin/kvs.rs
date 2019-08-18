extern crate clap;
use clap::{App, Arg, SubCommand};

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
        // ("set", Some(m)) => {},
        // ("set", Some(m)) => {},
        // ("set", Some(m)) => {},
        _ => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
    }
}
