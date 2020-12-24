use clap::Clap;

// subcommands to implement
// data
// alerts
// set
// publish
// profile

// cargo run -- get --id 0x8877
// cargo run -- get --all

// cargo run -- profile --input filename.csv
// cargo run -- profile -i filename.csv

// xtract profile --input=mydata.csv --output=meta.txt --publish=true
// xtract profile --input=s3://mydata.csv --output=meta.txt --publish=true
// xtract profile -i ./data/filename.csv --name custom_name.csv --publish

// Get all alerts of data_id = 0x1234
// xtract alerts --data 0x1234

// Get single alert with id
// xtract alerts --id 0xabcd

// Flag to delete alerts in the request
// xtract alerts --data 0x1234 --delete
// xtract alerts --id 0xabcd --delete

#[derive(Clap, Clone)]
pub struct Data {
    #[clap(long)]
    pub id: Option<String>,

    #[clap(long, conflicts_with = "id", takes_value = false)]
    pub all: Option<bool>,

    #[clap(long, takes_value = false)]
    pub delete: bool,
}

#[derive(Clap, Clone)]
pub struct Alerts {
    #[clap(long)]
    pub id: Option<String>,

    #[clap(long, conflicts_with = "id")]
    pub data: Option<String>,

    #[clap(long, takes_value = false)]
    pub delete: bool,
}

#[derive(Clap, Clone)]
pub struct Profile {
    #[clap(short, long)]
    pub input: Option<String>,

    // #[clap(long, conflicts_with="id", takes_value=false)]
    #[clap(long, takes_value = false)]
    pub publish: bool,
}

#[derive(Clap)]
pub enum SubCommand {
    #[clap(version = "0.0.1", author = "francesco@amethix.com")]
    /// Login to remote service
    Login,
    /// Get remote assets
    Data(Data),
    /// Get alerts
    Alerts(Alerts),
    /// Set metadata of remote assets
    Set,
    /// Publish metadata to remote service
    // Publish,
    /// Profile of data passed as argument
    Profile(Profile),
}

#[derive(Clap)]
#[clap(
    version = "0.0.1",
    author = "Author: Francesco Gadaleta <francesco@amethix.com>"
)]
pub struct Args {
    #[clap(subcommand)]
    pub subcmd: SubCommand,

    #[clap(long)]
    pub input: Option<String>,

    #[clap(short, long)]
    _verbose: Option<i32>,
}
