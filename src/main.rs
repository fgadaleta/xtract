mod cli;
mod client;
mod configuration;
mod loaders;

use clap::Clap;
use anyhow::Result;


fn main() -> Result<()> {
    let args: cli::Args = cli::Args::parse();
    let client = client::Frontend::new(args);
    client.run()
}


// #[test]
// fn check_answer_validity() {
//     assert_eq!(true, true);
// }