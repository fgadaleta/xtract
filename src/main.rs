use clap::Clap;
use anyhow::Result;

pub mod cli;
pub mod client;
// use xtract::{cli, client} ;


fn main() -> Result<()> {
    let args: cli::Args = cli::Args::parse();
    let client = client::Frontend::new(args);
    client.run()
}


// #[cfg(test)]
// mod tests {
//     use super::*;
//     use test::Bencher;

//     #[test]
//     fn it_works() {
//         assert_eq!(4, add_two(2));
//     }

//     #[bench]
//     fn bench_add_two(b: &mut Bencher) {
//         b.iter(|| add_two(2));
//     }
// }