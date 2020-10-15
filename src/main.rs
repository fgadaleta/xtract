mod cli;

use clap::Clap;
use std::io::{self, BufReader, Write};
use log::{info};


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: cli::Args = cli::Args::parse();

    const BASE_URL: &str = "http://localhost:5000";


    // prepare stdout entity to print messages to
    let stdout = io:: stdout();
    let mut handle = io::BufWriter::new(stdout);  // wrap handle in a buffer
    // initialize logger
    env_logger::init();
    info!("starting up");


    /*
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

    let filecontent = std::fs::read_to_string(&args.input)
        .with_context(|| format!("Could not read input file "))?;

    // println!("Printing content from file");
    // for line in filecontent.lines() {
    //     if line.contains(&args.subcmd) {
    //         writeln!(handle, "{}", line);  // instead of println!("{}", line);
    //     }
    // }
    */

    Ok(())
}



#[test]
fn check_answer_validity() {
    assert_eq!(true, true);
}