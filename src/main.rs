// Run as:
// cargo run -- info README.md --output result.txt --verbose
//

use std::fmt;
// use std::env::args;
use std::io::{self, BufReader, Write};
// use std::fs::File;
use clap_verbosity_flag;
use structopt::StructOpt;
use anyhow::{Context, Result};
use indicatif::ProgressBar;
use log::{info, warn};
use env_logger;

#[derive(Debug)]
struct Error(String);


#[derive(StructOpt)]
struct Cli {
    /// Action to execute
    action: String,
    /// Input file to process
    #[structopt(parse(from_os_str))]
    path: std::path::PathBuf,
    /// Output file to save results
    #[structopt(short="o", long="output")]
    output: std::path::PathBuf,
    #[structopt(flatten)]
    verbose: clap_verbosity_flag::Verbosity,
}

impl fmt::Debug for Cli {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cli")
        .field("action", &self.action)
        .field("path", &self.path)
        .field("output", &self.output)
        .finish()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::from_args();

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

    let filecontent = std::fs::read_to_string(&args.path)
        .with_context(|| format!("Could not read input file "))?;

    println!("Printing content from file");
    for line in filecontent.lines() {
        if line.contains(&args.action) {
            writeln!(handle, "{}", line);  // instead of println!("{}", line);
        }
    }

    Ok(())
}



#[test]
fn check_answer_validity() {
    assert_eq!(answer(), 42);
}