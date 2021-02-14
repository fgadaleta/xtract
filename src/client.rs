use super::cli::{Args, SubCommand, Alert, TriggerSubCommand };
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use serde::{Serialize, Deserialize};
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation, Algorithm};
use encoding_rs::*;

// use serde::de::{
//     DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess,
//     VariantAccess, Visitor,
// };
// use home_dir;
use std::{collections::HashMap, fmt::format};
use std::time::{SystemTime, UNIX_EPOCH};
// use std::fs;
// use std::path::Path;
use std::env::var;
use std::path::PathBuf;
use std::fs::File;
use std::io::prelude::*;
use std::io::Cursor;
use std::sync::Arc;
use std::ops::Not;
use std::process;
use tokio::runtime::Runtime;

// from https://edgarluque.com/blog/wrapping-errors-in-rust
use thiserror::Error;

use xtract::configuration::{get_configuration_from_file, get_content_from_file};
use xtract::loaders::s3_connector::Storage;
// TODO remove and use only polars DataFrame
use xtract::loaders::dataframe::NcodeDataFrame;
use xtract::loaders::frame::DataFrame;
// use crate::transformers::simple;
use xtract::loaders::csv_format::CsvReader as csvr;
use polars::prelude::*;
use polars::frame::ser::csv::CsvEncoding;

use sqlparser::dialect::{GenericDialect, keywords::SQL};
use sqlparser::parser::Parser;
use sqlparser::ast::{Statement};
use sqlparser::tokenizer::Tokenizer;
use futures::FutureExt;

use datafusion::prelude::*;
// use datafusion::error::Result;

use tokio;

// use arrow::datatypes::DataType;
// #[cfg(feature = "prettyprint")]
// use arrow::util::print_batches;

// TODO move to config.rs or something
static RT: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());
const CONFIG_DIR: &str = ".ncode";
const CONFIG_FILENAME: &str = "configuration.toml";
const CONFIG_SAMPLE_FILENAME: &str = "configuration-sample.toml";
static CONFIG_SAMPLE_CONTENT: &'static str = "
[api]
server = \"api.ncode.ai\"
port = 5000
max-age = 5400

[credentials]
username = \"username\"
password = \"password\"

[settings]

[storage]
url = \"http://my-s3-storage\"
access_key = \"access_key\"
secret_access_key = \"secret_access_key\"
";

#[derive(Debug, Deserialize, Serialize)]
struct Claims {
    sub: String,
    role: Option<String>,
    exp: usize,
}

impl Claims {
    pub fn expire(&self) -> usize {
        self.exp
    }
}


/*

"my_and_rule": Object({
        "all": Array([String("b>2",), String("c=3",),]),
    }),

*/

// user defined rule
#[derive(Debug, Deserialize, Serialize)]
struct Rule {
    rulename: String,
    conditions: Conditions, // HashMap<String, Vec<String>>
}

#[derive(Debug, Deserialize, Serialize)]
struct Conditions {
    #[serde(default)]
    any: Vec<String>,
    #[serde(default)]
    all: Vec<String>,
    #[serde(default)]
    not: Vec<String>,
}

pub struct Frontend {
    args: Args,
}

// #[cfg(feature = "async_await")]
#[derive(Serialize, Deserialize, Debug)]
struct DataResponse {
    id: String,
    r#type: String,
    datastore: String,
    filename: String,
    submitted_by: String,
    trigger_id: Option<String>,
    _is_published: Option<String>,
    _submitted_on: String,
    _id: Option<String>,
    _key: Option<String>,
    _rev: Option<String>
}

// type AlertBody = HashMap<String, String>;

#[derive(Serialize, Deserialize, Debug)]
pub struct Body {
    timestamp: String,
    profile_id: String,
    r#type: String,
    //#[serde(deserialize_with = "...")]
    message: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AlertResponse {
    data_id: String,
    body: Body,
    created_on: String,
    id: String,
    _id: Option<String>,
    _key: Option<String>,
    _rev: Option<String>
}

impl AlertResponse {
    pub fn get_timestamp(&self) -> String {
        self.body.timestamp.to_owned()
    }

    // pub fn get_body(&self) -> &Body {
    //     &self
    // }
}


// pub struct Rule {
//     id: String,
//     rulename: String,
//     conditions: HashMap<String, Vec<String>>
// }

#[derive(Serialize, Deserialize, Debug)]
pub struct TriggerResponse {
    rules: Vec<Rule>,
    r#type: Option<String>,
    _submitted_by: String,
    _submitted_on: String,
    id: String,
    _id: Option<String>,
    _key: Option<String>,
    _rev: Option<String>
}

fn alerts_from_custom_rules(inputpath: &String, rules: &Value, columns: Vec<&str>) -> Vec<Alert> {

    let rules: Vec<Rule> = serde_json::from_value(rules.to_owned()).unwrap();

    // println!("--------------------");
    // println!("DBG rules: {:?}", &rules);
    // println!("--------------------");

    // create local execution context
    let mut ctx = ExecutionContext::new();

    // register csv file with the execution context
    // ctx.register_csv(
    //     "virtual_table",
    //     &inputpath,
    //     CsvReadOptions::new(),)
    //     .unwrap();

    match ctx.register_csv("virtual_table", &inputpath, CsvReadOptions::new()) {
            Ok(x) => x,

            Err(e) => {
                println!("Error loading data. Only UTF-8 is supported.");
                println!("Error: {:?}", e);
                process::exit(-1);
            }
    };


    let mut alerts: Vec<Alert> = vec![];

    for rule in rules.iter() {
        // println!("\n------------------------------------------");
        println!("----- Processing rule name: [{:?}] -----", &rule.rulename);
        let any = &rule.conditions.any;
        let all = &rule.conditions.all;
        let not = &rule.conditions.not;

        // if zero conditions, move to next rule
        if any.len() + all.len() + not.len() == 0 {
            break;
        }

        // generate sql code from rules
        let mut sql_select_from = format!("SELECT * FROM virtual_table ");
        let mut sql_where = format!("WHERE ");

        // generate from ANY rules
        for (i, cond) in any.iter().enumerate() {
            sql_where.push_str(&cond[..]);
            if i < any.len()-1 {
                sql_where.push_str(" OR ");
            }
        }

        // generate from ALL rules
        for (i, cond) in all.iter().enumerate() {
            sql_where.push_str(&cond[..]);
            if i < all.len()-1 {
                sql_where.push_str(" AND ");
            }
        }

        // TODO generate from NOT rules
        for (i, cond) in not.iter().enumerate() {
            sql_where.push_str(&cond[..]);
            if i < not.len()-1 {
                sql_where.push_str(" NOT ");
            }
        }

        sql_select_from.push_str(&sql_where[..]);
        println!("DBG generated query: {:?}", &sql_select_from);

        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, &sql_select_from[..]).unwrap();
        let mut tok = Tokenizer::new(&dialect, &sql_select_from);
        let toks = tok.tokenize().unwrap();
        // println!("DBG tokens: {:?}", toks);

        // println!("DBG AST len: {:?}", ast.len());
        for stm in ast.iter() {
            // dbg!("DBG AST statement: {:?}", &stm);
            match stm {
                Statement::Query(q) => {
                    // println!("DBG AST parsed query {:?}", &q);
                    let exp = &q.body;

                    // match exp {

                    // }

                },

                _ => println!("not a valid query")
            }
        }

        // execute this rule
        let df = ctx.sql(&sql_select_from[..]).unwrap();

        let results = async {
            let res = df.collect().await;
            let nelem = match res {
                Ok(s) => {
                    let cols = s[0].columns();
                    let sa = &cols[0];
                    let s = sa.data().len();
                    s
                },

                Err(_) => {
                    println!("DBG error");
                    0 as usize
                }
            };
            nelem
        };

        let trigger_alert = RT.handle().block_on(results);
        // println!("DBG {:?} elements triggered alert\n", trigger_alert);
        let alert_data = format!("DBG {:?} elements triggered alert", trigger_alert);
        println!("\t-> {} elements triggered alert", trigger_alert);
        let alert = Alert::new(None, Some(alert_data));
        alerts.push(alert);
        // results = results.map(|x| { println!("{:?}", x); x});

        println!("\n\n");
    }

    alerts

}

impl Frontend {
    pub fn new(args: Args) -> Self {
        Frontend { args }
    }

    pub fn run(&self) -> Result<()> {
        // check if exists
        let mut config_path = PathBuf::from(var("HOME").unwrap());
        config_path.push(CONFIG_DIR);

        let mut config_file_path = config_path.clone();
        config_file_path.push(CONFIG_FILENAME);

        let mut token_file_path = config_path.clone();
        token_file_path.push(".token");

        let config_file_path = config_file_path
            .into_os_string()
            .into_string()
            .unwrap();
        let config_path_exist = config_path.exists();

        if !config_path_exist {
            println!("Creating ncode home folder for the first time...");
            let config_path = config_path
                .clone()
                .into_os_string()
                .into_string()
                .unwrap();

            std::fs::create_dir_all(config_path.clone()).unwrap();
            let mut file = File::create(format!("{}/{}", config_path, CONFIG_SAMPLE_FILENAME))
                .unwrap();
            file.write_all(CONFIG_SAMPLE_CONTENT.as_bytes()).unwrap();
            println!("done.");
        }

        // get configuration
        let config = get_configuration_from_file(&config_file_path[..])?;

        // TODO different type dispatcher

        // get API url
        let url = format!("http://{}:{}", config.api.server, config.api.port);
        let tokenfile = token_file_path.into_os_string().into_string().unwrap();

        // check token is still valid
        let token = get_content_from_file(&tokenfile[..])?;
        let now = SystemTime::now();
        let now = now
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs();

        // let dectoken = decode::<Claims>(&token, &DecodingKey::from_secret("my_precious_secret_key".as_ref()), &Validation::default()).unwrap();
        // let expdate = dectoken.claims.expire() as u64;
        // let valid_for = expdate - now;
        let valid_for = 2;
        // check valid or exit
        if valid_for < 1 {
            println!("JWT token no longer valid. Please login again.");
            process::exit(-1);
        }

        match &self.args.subcmd {
            SubCommand::Login => {
                // get credentials
                let credentials = json!({
                    "username": config.credentials.username,
                    "password": config.credentials.password
                });

                // login and get token
                let token = self.login_helper(url, credentials)?;

                let mut file = File::create(tokenfile)?;
                file.write_all(token.as_bytes())?;
                Ok(())
            }

            SubCommand::Data(t) => {
                let mut res: HashMap<String, String> = HashMap::new();
                let delete_data = t.delete;

                let get_all = match t.all {
                    Some(opt) => opt,
                    None => false,
                };

                if get_all {
                    let endpoint = format!("{}/data", url);
                    res = self.get_helper(endpoint, token.clone())?;

                } else {
                    // fetching only one asset with id
                    let id_to_fetch = match t.id.as_ref() {
                        Some(id) => String::from(id),
                        None => String::from(""),
                    };

                    let endpoint = format!("{}/data/{}", url, id_to_fetch);
                    res = self.get_helper(endpoint, token.clone())?;
                }

                if res.contains_key("status").not() {
                    println!("Error establishing connection.\nServer can be down.");
                    process::exit(1);
                }

                match &res["status"][..] {
                    "success" => {
                        let data_assets = serde_json::from_str::<Vec<DataResponse>>(res["message"].as_str()).unwrap();
                        for (i, asset) in data_assets.iter().enumerate() {
                            println!("\n********** DATA ASSET {} **********", i);
                            println!("id: {}", asset.id);
                            println!("type: {}", asset.r#type);
                            println!("filename: {}", asset.filename);
                            println!("submitted_on: {}", asset._submitted_on);
                            println!("datastore: {}", asset.datastore);

                            // print trigger_id if available
                            let trigger_id = match &asset.trigger_id {
                                Some(tid) => tid.to_owned(),
                                None => String::from("NA")
                            };
                            println!("trigger_id: {}", trigger_id);


                            if delete_data {
                                let endpoint = format!("{}/data/{}", url, asset.id);
                                let _res = self.del_helper(endpoint, token.clone());
                            }
                        }
                    },
                    _ => println!("Status not OK"),
                }

                Ok(())
            }

            SubCommand::Alert(t) => {
                let mut data_id: String = "".to_string();
                let mut alert_id: String = "".to_string();
                let mut res: HashMap<String, String> = HashMap::new();

                let get_all_alerts: bool = match t.data.as_ref() {
                    Some(did) => {
                        data_id = did.to_string();
                        true
                    },
                    None => false
                };
                let get_single_alert: bool = match t.id.as_ref() {
                    Some(aid) => {
                        alert_id = aid.to_string();
                        true
                    },
                    None => false
                };
                // exit if no flag is passed
                if !(get_all_alerts || get_single_alert) {
                    println!("Must pass at least one flag --data or --id ");
                    process::exit(-1);
                }
                let delete_alert = t.delete;
                // prepare endpoints here and call get_helper for all and single
                if get_all_alerts {
                    let endpoint = format!("{}/data/{}/alerts", url, data_id);
                    res = self.get_helper(endpoint, token.clone())?;
                    if delete_alert {
                        println!("TODO create endpoint DEL /alerts/:id for each :id");
                    }
                }
                if get_single_alert {
                    let endpoint = format!("{}/alerts/{}", url, alert_id);
                    res = self.get_helper(endpoint, token.clone())?;
                    if delete_alert {
                        println!("TODO create endpoint DEL /alerts/:id");
                    }
                }
                match &res["status"][..] {
                    "success" => {
                        let alerts = serde_json::from_str::<Vec<AlertResponse>>(res["message"].as_str()).unwrap();
                        if alerts.len() ==  0 {
                            println!("No alerts for this data asset.")
                        }
                        else {
                            for (i, alert) in alerts.iter().enumerate() {
                                println!("\n********** ALERT {} ********** ", i);
                                println!("data_id: {}", alert.data_id);
                                println!("created_on: {}", alert.created_on);
                                println!("alert_id: {}", alert.id);
                                let body = &alert.body;
                                println!("timestamp: {}", &body.timestamp);
                                println!("alert_type: {}", &body.r#type);
                                let messages: &Vec<String> = &body.message;
                                for (j, message) in messages.iter().enumerate() {
                                    println!("\n\t____________________ msg {} ____________________ \n", j);
                                    println!("\t{}", message);
                                }

                                if delete_alert {
                                    let endpoint = format!("{}/alerts/{}", url, alert.id);
                                    let _res = self.del_helper(endpoint, token.clone());
                                }
                            }
                        }
                    },
                    _ => println!("Cannot retrieve alert(s). Status not OK"),
                }
                Ok(())
            },

            SubCommand::Trigger(t) => {
                 // fetch and execute subcommand
                 let subcommand = t.clone().subcmd;

                 match subcommand {
                    TriggerSubCommand::CreateTrigger(t) => {
                        let input_to_fetch = t.input;
                        let mut rulesfile = File::open(input_to_fetch.clone()).expect("could not read file");
                        // Read the input file to string.
                        // let mut rulesfile = File::open("./data/user_defined_rules.json")?;
                        let mut rulescontents = String::new();
                        rulesfile.read_to_string(&mut rulescontents)?;
                        // println!("DBG rulescontents:{:?}", &rulescontents);
                        let rules: Value = serde_json::from_str(&rulescontents)?;
                        // println!("DBG rules: {:?}", &rules["rules"]);
                        // let rules = &rules["rules"];
                        // println!("DBG rules from file: {:?}", rules);

                        if  t.publish {
                            // post profile to new url
                            let post_data_endpoint = format!("{}/triggers/", url);
                            let res: HashMap<String, String> = self
                                .post_helper(post_data_endpoint, token.clone(), rules)
                                .unwrap();

                                let status = res.get("status").unwrap();

                                match res.get("trigger_id") {
                                    Some(tid) => {
                                        // println!("DBG in match did: {}", tid);
                                        println!("status: {}", status);
                                        println!("message: Trigger created successfully.");
                                        println!("trigger_id: {}", tid);
                                    },

                                    None => {
                                        println!("No trigger_id returned from server. Please login or contact an administrator at hello@ncode.ai");
                                        process::exit(1);
                                    }
                                }
                            }
                    },

                    TriggerSubCommand::GetTrigger(t) => {
                        // fetch get_all triggers flag
                        let get_all_triggers = t.all;

                        // fetch get trigger id
                        let mut data_id = String::new();
                        let get_single_data_triggers = match t.clone().data {
                            Some(did) => {
                                data_id = did;
                                true
                            },
                            None => false
                        };

                        // fetch delete flag
                        let delete_trigger = t.delete;
                        let mut res: HashMap<String, String> = HashMap::new();
                        if get_all_triggers {
                            let endpoint = format!("{}/triggers", url);
                            res = self.get_helper(endpoint, token.clone())?;

                            if !res.contains_key("status") {
                                println!("Error establishing connection.\nServer can be down.");
                                process::exit(1);
                            }
                            match &res["status"][..] {
                                "success" => {
                                    let all_triggers = serde_json::from_str::<Vec<TriggerResponse>>(res["message"].as_str()).unwrap();
                                    if all_triggers.len() ==  0 {
                                        println!("No triggers found.")
                                    }
                                    else {
                                        for (i, trigger) in all_triggers.iter().enumerate() {
                                            println!("\n********** TRIGGER {} ********** ", i);
                                            println!("trigger_id: {}", trigger.id);
                                            println!("created_on: {}", trigger._submitted_on);
                                            let trigger_type = match &trigger.r#type {
                                                Some(t) => t.to_owned(),
                                                None=> "undefined".to_string()
                                            };
                                            println!("type: {}", trigger_type);
                                            let rules = &trigger.rules;
                                            for (_j, rule) in rules.iter().enumerate() {
                                                println!("rulename: {}", &rule.rulename);
                                                let conds = &rule.conditions;
                                                let any = &conds.any;
                                                let all = &conds.all;
                                                let _not = &conds.not;
                                                if any.len() > 0 {
                                                    println!("\n\t ANY condition applies \n");
                                                    for (_j, cond) in any.iter().enumerate() {
                                                        println!("\t\t{}", cond);
                                                    }
                                                }
                                                if all.len() > 0 {
                                                    println!("\n\t ALL conditions apply \n");
                                                    for (_j, cond) in all.iter().enumerate() {
                                                        println!("\t\t{}", cond);
                                                    }
                                                }
                                            }

                                            if delete_trigger {
                                                let endpoint = format!("{}/triggers/{}", url, trigger.id);
                                                let _res = self.del_helper(endpoint, token.clone());
                                            }
                                        }
                                    }
                                },
                                _ => println!("Status not OK"),
                            }
                        }
                        // Implement GET /data/:id/triggers endpoint
                        else if get_single_data_triggers {
                            let endpoint = format!("{}/data/{}/triggers", url, data_id);
                            res = self.get_helper(endpoint, token.clone())?;
                            if !res.contains_key("status") {
                                println!("Error establishing connection.\nServer can be down.");
                                process::exit(1);
                            }

                            match &res["status"][..] {
                                "success" => {
                                    let single_data_triggers = serde_json::from_str::<Vec<TriggerResponse>>(res["message"].as_str()).unwrap();
                                    println!("DBG single_data_triggers {:?} ", &single_data_triggers);

                                    if single_data_triggers.len() ==  0 {
                                        println!("No triggers found.")
                                    }
                                    else {
                                        for (i, trigger) in single_data_triggers.iter().enumerate() {
                                            println!("\n********** TRIGGER {} ********** ", i);
                                            println!("trigger_id: {}", trigger.id);
                                            println!("created_on: {}", trigger._submitted_on);
                                            let trigger_type = match &trigger.r#type {
                                                Some(t) => t.to_owned(),
                                                None=> "undefined".to_string()
                                            };
                                            println!("type: {}", trigger_type);
                                            let rules = &trigger.rules;
                                            for (_j, rule) in rules.iter().enumerate() {
                                                println!("rulename: {}", &rule.rulename);
                                                let conds = &rule.conditions;
                                                let any = &conds.any;
                                                let all = &conds.all;
                                                let _not = &conds.not;
                                                if any.len() > 0 {
                                                    println!("\n\t ANY condition applies \n");
                                                    for (_j, cond) in any.iter().enumerate() {
                                                        println!("\t\t{}", cond);
                                                    }
                                                }
                                                if all.len() > 0 {
                                                    println!("\n\t ALL conditions apply \n");
                                                    for (_j, cond) in all.iter().enumerate() {
                                                        println!("\t\t{}", cond);
                                                    }
                                                }
                                            }

                                            if delete_trigger {
                                                let endpoint = format!("{}/triggers/{}", url, trigger.id);
                                                let _res = self.del_helper(endpoint, token.clone());
                                            }
                                        }
                                    }

                                },
                                _ => println!("Status not OK"),
                            }
                        }
                    },

                    // POST /data/<data_id>/triggers and payload = { trigger_id: 0x123 }
                    TriggerSubCommand::SetTrigger(t) => {
                        let post_data_endpoint = format!("{}/data/{}/triggers", url, t.data_id);
                        let payload = serde_json::json!({"trigger_id": t.trigger_id});
                        let res: HashMap<String, String> = self
                            .post_helper(post_data_endpoint, token.clone(), payload)
                            .unwrap();
                        let status = res.get("status").unwrap();
                        let message = res.get("message").unwrap();
                        let message: HashMap<String, String> = serde_json::from_str(message).unwrap();
                        let message = message.get("message").unwrap();

                        println!("\nstatus: {}", status);
                        println!("message: {}\n", message);
                    }
                 }

                Ok(())
            },

            SubCommand::Profile(t) => {
                let input_to_fetch = &t.input;
                let publish_to_api = t.clone().publish;
                println!("Publish after profile: {:?}", publish_to_api);

                // Read the input file to string.
                let mut rulesfile = File::open("./data/user_defined_rules.json")?;
                let mut rulescontents = String::new();
                rulesfile.read_to_string(&mut rulescontents)?;
                // println!("DBG rulescontents:{:?}", &rulescontents);
                let filerules: Value = serde_json::from_str(&rulescontents)?;
                // println!("DBG filerules: {:?}", &filerules["rules"]);
                let rules = &filerules["rules"];

                // let rules = &raw_rules["rules"];
                // println!("DBG rules: {:?}", &rules);

                let input_location: String = input_to_fetch.chars().skip(0).take(5).collect();
                match input_location.as_ref() {
                    // try s3 bucket
                    "s3://" => {
                        unimplemented!();
                        // // TODO integrate DataFrame
                        // let filename: String = input_to_fetch.chars().skip(5).collect();
                        // println!("filename input_to_fetch: {}", filename);
                        // let storage = Storage::new();
                        // // TODO get this from input_to_fetch
                        // // let filename = String::from("synthetic_demo_data.csv");
                        // let df = self.csv_reader_helper(storage, filename);
                        // let _profile = df.profile();
                        // // println!("Dataset profile: {}", profile);
                    }

                    // try local file
                    _ => {
                        let mut file = File::open(&input_to_fetch.clone()[..]).unwrap();
                        let mut src = Vec::new();
                        let read = file.read_to_end(&mut src).unwrap();
                        let size: usize = src.len();
                        // let mut buf: Vec<u8> = Vec::with_capacity(100 * 1024 * 1024);

                        let (src, encoding) =
                            if let Some((encoding, skip)) =
                                encoding_rs::Encoding::for_bom(&src) {
                                    (&src[skip..], encoding)
                            } else {
                                let mut detector = chardetng::EncodingDetector::new();
                                detector.feed(&src, true);
                                (&*src, detector.guess(None, true))
                        };

                        // println!("DBG src:{:?} enc: {:?}", &src, &encoding);
                        let mut decoder = encoding.new_decoder_without_bom_handling();
                        let mut string = String::with_capacity(decoder.max_utf8_buffer_length(src.len()).unwrap());
                        let (res, read, _replaced) = decoder.decode_to_string(&src, &mut string, true);
                        // println!("DBG string:{:?}", &string);

                        // TODO load columns into Series
                        // TODO load Series into DataFrame
                        // TODO then get rid of CsvReader below as you will already have the dataframe

                        // input_to_fetch is a local file
                        let file = File::open(input_to_fetch.clone()).expect("could not read file");
                        let df = CsvReader::new(file)
                            .infer_schema(None)
                            .has_header(true)
                            .with_encoding(CsvEncoding::LossyUtf8)
                            .finish();

                        let df = match df {
                            Ok(d) => d,

                            Err(e) => {
                                println!("Could not load dataframe. Only UTF-8 is supported.");
                                println!("Error {:?}", e);
                                process::exit(-1);
                            }
                        };

                        let dataframe = NcodeDataFrame {
                            dataframe: Arc::new(df),
                        };

                        let columns = dataframe.columns();
                        // TODO validate rules with colnames

                        let alerts = alerts_from_custom_rules(input_to_fetch, rules, columns);

                        // print alerts
                        println!("----- User Defined Alerts -----");
                        for (i,alert) in alerts.iter().enumerate() {
                            println!("UDA-{}: {:?}\n", i, &alert);
                        }

                        // TODO Result(profile)
                        let mut profile = dataframe.profile();

                        // add filename to profile
                        profile.set_datasource(input_to_fetch.clone());
                        // Convert to string and print
                        // let profile_str = serde_json::to_string_pretty(&profile).unwrap();
                        let profile_str = serde_json::to_value(&profile).unwrap();
                        println!("Profile: {}", &profile_str);

                        if publish_to_api {
                            // post profile to new url
                            let post_data_endpoint = format!("{}/data/", url);
                            let data_body = json!({"type": "local", "filename": format!("{}", input_to_fetch.clone()) });
                            // println!("DBG body: {:?}", data_body);
                            // println!("DBG body.to_string(): {:?}", data_body.to_string());

                            let res: HashMap<String, String> = self
                                .post_helper(post_data_endpoint, token.clone(), data_body)
                                .unwrap();

                            // println!("DBG POST req res: {:?}", &res);
                            // println!("data_id: {:?}", res.get("data_id"));

                            // get data_id from response
                            match res.get("data_id") {
                                Some(did) => {
                                    // println!("DBG in match did: {}", did);

                                    let post_profile_endpoint = format!("{}/data/{}/profile", url, did);
                                    let profile_res = self
                                         .post_helper(post_profile_endpoint, token.clone(), json!(profile_str))
                                         .unwrap();

                                    // println!("DBG profile_res {:?}", &profile_res);

                                    let status = profile_res.get("status"); // .unwrap();

                                    match status {
                                        Some(s) => {
                                            println!("status: {}", s);
                                            println!("message: {}", profile_res.get("message").unwrap());
                                            },

                                        _ => {
                                            println!("status: None");
                                            // println!("message: {}", profile_res.get("message").unwrap());
                                        },
                                    }

                                    // match profile_res.get("message") {
                                    //     Some(msg) => {
                                    //         println!("{}\n", msg);
                                    //     },
                                    //     None => {
                                    //         println!("Something went wrong ");
                                    //         process::exit(1);
                                    //     }
                                    // }

                                },

                                None => {
                                    println!("No data_id returned from server. Contact an administrator at hello@ncode.ai");
                                    process::exit(1);
                                }
                            }

                            // let post_profile_endpoint = format!("{}/data/{}/profile", url, data_id);
                            // println!("DBG ready to hit endpoint {:?} ", post_profile_endpoint);
                            // let res = self
                            //     .post_helper(post_profile_endpoint, tokenfile, json!(profile_str))
                            //     .unwrap();
                            // println!("DBG POST req res: {:?}", res);
                        }
                    }
                }
                Ok(())
            }
        }
    }

    // TODO rename this to csv_reader_from_s3
    /// Get filename from S3 bucket (storage) before profiling
    ///
    fn csv_reader_helper(&self, storage: Storage, filename: String) -> DataFrame {
        let fut = async { storage.get_object(filename).await };
        let (data, data_type) = RT.handle().block_on(fut);
        println!("content type: {}", data_type);

        let data_str = String::from_utf8(data).unwrap();
        let file = Cursor::new(&data_str[..]);
        // from here on it's like a regular file descriptor
        let mut reader = csvr::new(file)
            .infer_schema(100)
            .has_header(true)
            .with_batch_size(128)
            .finish();

        let mut batches: Vec<RecordBatch> = vec![];
        let mut has_next = true;
        while has_next {
            match reader.next().transpose() {
                Ok(batch) => match batch {
                    Some(batch) => batches.push(batch),
                    None => {
                        has_next = false;
                    }
                },

                Err(_) => {
                    has_next = false;
                }
            }
        }
        // println!("batches {:?}", batches);
        let schema = batches[0].schema();
        // println!("single batch schema: {} ", schema);
        // println!("num batches: {}", batches.len());
        let df = DataFrame::from_record_batches(schema, batches);
        df
    }

    fn login_helper(
        &self,
        url: String,
        payload: serde_json::Value,
    ) -> Result<String, reqwest::Error> {
        let endpoint = format!("{}/auth/login", url);
        let http_client = reqwest::blocking::ClientBuilder::new().build()?;
        let response = http_client.post(&endpoint).json(&payload).send();

        // println!("DEBUG {:?}", response);
        match response {
            Ok(res) => {
                let mut result = String::new();
                if res.status() == reqwest::StatusCode::OK {
                    let auth_token = res.json::<HashMap<String, String>>()?;
                    result = auth_token["Authorization"].to_string();

                } else {
                    println!("Response not 200 OK :(");
                    process::exit(-1);
                }
                println!("Successfully logged in!\nWelcome to ncode!\n\n");
                Ok(result)
            }

            Err(e) => {
                println!("Could not make the request! (Oops..)");
                Err(e)
            }
        }
    }

    fn get_helper(&self, endpoint: String, token: String) -> Result<HashMap<String, String>> {
        // get token and send to request as is (encoding occurs server-side)
        // let token = get_content_from_file(&tokenfile[..])?;
        // let token = base64::encode(token);

        let http_client = reqwest::blocking::ClientBuilder::new().build()?;
        let response = http_client
            .get(&endpoint)
            .header("Authorization", format!("{}{}", "Bearer ", token))
            .send();

        // let mut response: String = "".to_string();
        let mut result: HashMap<String, String> = HashMap::new();

        match response {
            Ok(res) => {
                if res.status() == reqwest::StatusCode::OK {
                    println!("status ok ");
                    let str_assets: String = res.text()?;
                    // let assets: String = res.json()?;
                    result.insert("status".to_string(), "success".to_string());
                    result.insert("message".to_string(), str_assets.clone());

                } else {
                    println!("Status not ok");
                    // response = res.text()?;
                    result.insert("status".to_string(), "failed".to_string());
                    result.insert("message".to_string(), res.text()?);
                }
            }

            Err(e) => {
                println!("Could not make request! {:?} ", e);
            }
        }
        Ok(result)
    }

    fn del_helper(&self, endpoint: String, token: String) -> Result<HashMap<String, String>> {
        // get token and send to request as is (encoding occurs server-side)
        // let token = get_content_from_file(&tokenfile[..])?;
        let http_client = reqwest::blocking::ClientBuilder::new().build()?;
        let response = http_client
            .delete(&endpoint)
            .header("Authorization", format!("{}{}", "Bearer ", token))
            .send();

        // let mut response: String = "".to_string();
        let mut result: HashMap<String, String> = HashMap::new();

        match response {
            Ok(res) => {
                if res.status() == reqwest::StatusCode::OK {
                    println!("status ok ");
                    let str_assets: String = res.text()?;
                    // let assets: String = res.json()?;
                    result.insert("status".to_string(), "success".to_string());
                    result.insert("message".to_string(), str_assets.clone());

                } else {
                    println!("Status not ok");
                    // response = res.text()?;
                    result.insert("status".to_string(), "failed".to_string());
                    result.insert("message".to_string(), res.text()?);
                }
            }

            Err(e) => {
                println!("Could not make request! {:?} ", e);
            }
        }
        Ok(result)
    }

    fn post_helper(&self, endpoint: String, token: String, body: Value) -> Result<HashMap<String, String>> {
        let mut result: HashMap<String, String> = HashMap::new();
        // let token = get_content_from_file(&tokenfile[..])?;
        // dbg!("get_content_from_file token: {}", &token);
        // let token = base64::encode(token);
        let http_client = reqwest::blocking::ClientBuilder::new().build()?;

        // let body: HashMap<String, String> = body;

        let response = http_client
            .post(&endpoint)
            .header("Authorization", format!("{}{}", "Bearer ", token))
            .json(&body)
            .send();

        match response {
            Ok(res) => {
                if res.status() == reqwest::StatusCode::OK {
                    // let response: String = res.json()?;
                    let response: String = res.text()?;
                    result.insert("status".to_string(), "success".to_string());
                    result.insert("message".to_string(), response.clone());

                } else if res.status() == reqwest::StatusCode::CONFLICT {
                    result = res.json()?;

                } else if res.status() == reqwest::StatusCode::CREATED {
                    result = res.json()?;

                } else if res.status() == reqwest::StatusCode::NOT_FOUND {
                    let response: String = res.text()?;
                    result.insert("status".to_string(), "failed".to_string());
                    result.insert("message".to_string(), response.clone());
                }

            }

            Err(e) => {
                println!("Could not make request! {:?} ", e);
            }
        }

        Ok(result)
    }
}
