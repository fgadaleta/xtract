// Run as:
// cargo run -- info README.md --output result.txt --verbose
//

use std::fmt;
// use std::env::args;
use std::io::{self, BufReader, Write};
// use std::fs::File;
use clap::Clap;
use clap_verbosity_flag;
// use structopt::StructOpt;
use anyhow::{Context, Result};
use indicatif::ProgressBar;
use log::{info, warn};
use env_logger;

#[derive(Debug)]
struct Error(String);

#[derive(Clap)]
enum SubCommand {
    #[clap(version = "1.3", author = "Someone E. <someone_else@other.com>")]
    Test(Test),
}

/// A subcommand for controlling testing
#[derive(Clap)]
struct Test {
    /// Print debug info
    #[clap(short)]
    debug: bool
}

// #[derive(StructOpt)]
#[derive(Clap)]
#[clap(version = "0.1", author = "Francesco Gadaleta <francesco@amethix.com>")]
struct Cli {
    /// Action to execute
    // action: String,
    #[clap(subcommand)]
    subcmd: SubCommand,
    /// Input file to process
    // #[structopt(parse(from_os_str))]
    path: std::path::PathBuf,
    /// Output file to save results
    // #[structopt(short="o", long="output")]
    output: std::path::PathBuf,
    // #[structopt(flatten)]
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
}

impl fmt::Debug for Cli {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cli")
        // .field("subcmd", &self.subcmd)
        .field("path", &self.path)
        .field("output", &self.output)
        .finish()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let args = Cli::from_args();
    let args: Cli = Cli::parse();

    // prepare stdout entity to print messages to
    let stdout = io:: stdout();
    let mut handle = io::BufWriter::new(stdout);  // wrap handle in a buffer
    // initialize logger
    env_logger::init();
    info!("starting up");

    // progress bar
    let pb = ProgressBar::new(100);
    for i in 0..100 {
        // pb.println(format!("[+] finished #{}", i));
        pb.inc(1);
    }
    pb.finish_with_message("done");

    // dbg!("Parsed args: ", &args);
    // If action is "info", read file and extract info
    // todo use BufReader instead
    // https://doc.rust-lang.org/1.39.0/std/io/struct.BufReader.html

    // let result = std::fs::read_to_string(&args.path);
    // let filecontent = match result {
    //     Ok(content) => { content },
    //     Err(err) => { return Err(err.into());
    //                 // panic!("Cannot open file {}", err);
    //             }
    // };

    // dbg!("Action to perform {:?}", &args.subcmd);

    let filecontent = std::fs::read_to_string(&args.path)
        .with_context(|| format!("Could not read input file "))?;

    // println!("Printing content from file");
    // for line in filecontent.lines() {
    //     if line.contains(&args.subcmd) {
    //         writeln!(handle, "{}", line);  // instead of println!("{}", line);
    //     }
    // }

    Ok(())
}



#[test]
fn check_answer_validity() {
    assert_eq!(true, true);
}