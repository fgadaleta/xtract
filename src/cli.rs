use clap::Clap;


// subcommands to implement
// get
// set
// publish
// profile

// cargo run -- get --id 0x8877
// cargo run -- get --all

// cargo run -- profile --input filename.csv
// cargo run -- profile -i filename.csv

// xtract --input=mydata.csv --output=meta.txt --publish=true
// xtract --input=s3://mydata.csv --output=meta.txt --publish=true


#[derive(Clap, Clone)]
pub struct Get {
    #[clap(long)]
    pub id: Option<String>,

    #[clap(long, conflicts_with="id", takes_value=false)]
    pub all: Option<bool>,
}

#[derive(Clap, Clone)]
pub struct Profile {
    #[clap(short, long)]
    pub input: Option<String>,

    // #[clap(long, conflicts_with="id", takes_value=false)]
    // pub all: Option<bool>,
}

#[derive(Clap)]
pub enum SubCommand {
    #[clap(version = "0.0.1", author = "francesco@amethix.com")]
    /// Login to remote service
    Login,
    /// Get remote assets
    Get(Get),
    /// Set metadata of remote assets
    Set,
    /// Publish metadata to remote service
    Publish,
    /// Profile of data passed as argument
    Profile(Profile)
}

#[derive(Clap)]
#[clap(version = "0.0.1", author = "Author: Francesco Gadaleta <francesco@amethix.com>")]
pub struct Args {
    #[clap(subcommand)]
    pub subcmd: SubCommand,

    #[clap(long)]
    pub input: Option<String>,

    #[clap(short, long)]
    _verbose: Option<i32>,
}

