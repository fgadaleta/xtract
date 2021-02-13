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

// xtract trigger --input=myrules.json --publish=true

// Get all alerts of data_id = 0x1234
// xtract alert --data 0x1234

// Get single alert with id
// xtract alert --id 0xabcd

// Flag to delete alerts in the request
// xtract alert --data 0x1234 --delete
// xtract alert --id 0xabcd --delete

// Create triggers from user defined rule file
// xtract trigger --input data/user_defined_rules.json
// xtract trigger create --input data/user_defined_rules.json


// Set trigger to data
// xtract trigger set --data-id 0xABC --trigger-id 0x123

// Search data by criteria
// xtract search --cols "3,10" --rows "1000,3000" --tags "finance money transactions"

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
pub struct Trigger {
    #[clap(subcommand)]
    pub subcmd: TriggerSubCommand,

    #[clap(long)]
    pub data: Option<String>,

    #[clap(short, long)]
    pub input: Option<String>,

    #[clap(long, conflicts_with = "data", takes_value = false)]
    pub all: bool,

    #[clap(long, takes_value = false)]
    pub publish: bool,

    #[clap(long, takes_value = false)]
    pub delete: bool,
}


#[derive(Clap, Clone, Debug)]
pub enum TriggerSubCommand {
    /// A help message for the Test subcommand
    #[clap(name = "create", version = "0.1", author = "ncode")]
    CreateTrigger(CreateTrigger),

    #[clap(name = "set", version = "0.1", author = "ncode")]
    SetTrigger(SetTrigger),

    #[clap(name = "get", version = "0.1", author = "ncode")]
    GetTrigger(GetTrigger),

}

/// A subcommand for controlling testing
#[derive(Clap, Clone, Debug)]
pub struct CreateTrigger {
    #[clap(short, long)]
    pub input: String,

    #[clap(long, takes_value = false)]
    pub publish: bool,

    // /// Print debug info
    // #[clap(short = 'd')]
    // debug: bool
}

#[derive(Clap, Clone, Debug)]
pub struct SetTrigger {
    #[clap(short, long)]
    pub data_id: String,

    #[clap(short, long)]
    pub trigger_id: String,
}

#[derive(Clap, Clone, Debug)]
pub struct GetTrigger {
    #[clap(short, long)]
    pub id: Option<String>,

    #[clap(long)]
    pub data: Option<String>,

    #[clap(long, conflicts_with = "data", takes_value = false)]
    pub all: bool,

    #[clap(long, takes_value = false)]
    pub delete: bool,

}



#[derive(Clap, Clone, Debug)]
pub struct Alert {
    #[clap(long)]
    pub id: Option<String>,

    #[clap(long, conflicts_with = "id")]
    pub data: Option<String>,

    #[clap(long, takes_value = false)]
    pub delete: bool,
}

impl Alert {
    pub fn new(id: Option<String>, data: Option<String> ) -> Self {

        Alert {
            id,
            data,
            delete: false
        }
    }

}

#[derive(Clap, Clone)]
pub struct Profile {
    #[clap(short, long)]
    pub input: String,

    // #[clap(long, conflicts_with="id", takes_value=false)]
    #[clap(long, takes_value = false)]
    pub publish: bool,

    #[clap(long)]
    pub sql: Option<String>,

}

#[derive(Clap, Clone)]
pub struct Search {
    #[clap(long)]
    pub cols: Option<String>,

    #[clap(long)]
    pub rows: Option<String>,

    #[clap(long)]
    pub tags: Option<String>,

}


/// Return historical profiles from date to date for specific column
/// xtract history --from_date 10/11/2020 --to_date 31/12/2020 --column "Amount" --nullcount --mean
#[derive(Clap, Clone)]
pub struct History {
    #[clap(long)]
    pub column: String,

    #[clap(long)]
    pub from_date: String,
    #[clap(long)]
    pub to_date: String,

    #[clap(long, takes_value = false)]
    pub nunique: bool,
    #[clap(long, takes_value = false)]
    pub nullcount: bool,
    #[clap(long, takes_value = false)]
    pub categorical: bool,
    #[clap(long, takes_value = false)]
    pub min: bool,
    #[clap(long, takes_value = false)]
    pub max: bool,
    #[clap(long, takes_value = false)]
    pub mean: bool,
    #[clap(long, takes_value = false)]
    pub std: bool,
    #[clap(long, takes_value = false)]
    pub types: bool,
}



#[derive(Clap)]
pub enum SubCommand {
    #[clap(version = "0.0.1", author = "hello@ncode.ai")]
    /// Login to remote service
    Login,
    /// Get remote assets
    Data(Data),
    /// Get alerts
    Alert(Alert),
    /// Set metadata of remote assets
    // Set,
    /// Search data assets by criteria
    // Search(Search),
    /// Profile of data passed as argument
    Profile(Profile),
    Trigger(Trigger),
}

#[derive(Clap)]
#[clap(
    version = "0.0.1-alpha",
    author = "Team ncode.ai <hello@ncode.ai>"
)]
pub struct Args {
    #[clap(subcommand)]
    pub subcmd: SubCommand,

    #[clap(long)]
    pub input: Option<String>,

    #[clap(short, long)]
    _verbose: Option<i32>,
}
