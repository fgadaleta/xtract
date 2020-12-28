use super::cli::{Args, SubCommand};
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use serde::{Serialize, Deserialize};
// use serde::de::{
//     DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess,
//     VariantAccess, Visitor,
// };
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::Cursor;
use std::sync::Arc;
use std::ops::Not;
use std::process;
use tokio::runtime::Runtime;
use xtract::configuration::{get_configuration_from_file, get_content_from_file};
use xtract::loaders::s3_connector::Storage;
// TODO remove and use only polars DataFrame
use xtract::loaders::dataframe::NcodeDataFrame;
use xtract::loaders::frame::DataFrame;
// use crate::transformers::simple;
use xtract::loaders::csv_format::CsvReader as csvr;

use polars::prelude::*;
// use arrow::datatypes::DataType;

// #[cfg(feature = "prettyprint")]
// use arrow::util::print_batches;

static RT: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());

pub struct Frontend {
    args: Args,
}

#[cfg(feature = "async_await")]


#[derive(Serialize, Deserialize, Debug)]
struct DataResponse {
    id: String,
    r#type: String,
    datastore: String,
    filename: String,
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

impl Frontend {
    pub fn new(args: Args) -> Self {
        Frontend { args }
    }

    pub fn run(&self) -> Result<()> {
        // get configuration
        let config = get_configuration_from_file("./configuration.toml")?;

        // TODO different type dispatcher

        // get API url
        let url = format!("http://{}:{}", config.api.server, config.api.port);
        let tokenfile = config.settings.tokenfile;

        match &self.args.subcmd {
            SubCommand::Login => {
                // get credentials
                let credentials = json!({
                    "username": config.credentials.username,
                    "password": config.credentials.password
                });

                // login and get token
                let token = self.login_helper(url, credentials)?;

                // store token somewhere
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
                    res = self.get_helper(endpoint, tokenfile.clone())?;

                } else {
                    // fetching only one asset with id
                    let id_to_fetch = match t.id.as_ref() {
                        Some(id) => String::from(id),
                        None => String::from(""),
                    };

                    let endpoint = format!("{}/data/{}", url, id_to_fetch);
                    res = self.get_helper(endpoint, tokenfile.clone())?;
                }

                if res.contains_key("status").not() {
                    println!("Error establishing connection.\nServer can be down.");
                    process::exit(1);
                }

                match &res["status"][..] {
                    "success" => {
                        // TODO serde_json deserialize with Option<fields>

                        let data_assets = serde_json::from_str::<Vec<HashMap<String, String>>>(res["message"].as_str()).unwrap();
                        for (i, asset) in data_assets.iter().enumerate() {
                            println!("\n********** DATA ASSET {}", i);
                            println!("id: {}", asset["id"]);
                            println!("type: {}", asset["type"]);
                            println!("filename: {}", asset["filename"]);
                            println!("submitted_on: {}", asset["_submitted_on"]);
                            println!("datastore: {}", asset["datastore"]);

                            if delete_data {
                                let endpoint = format!("{}/data/{}", url, asset["id"]);
                                let res = self.del_helper(endpoint, tokenfile.clone());
                            }

                        }
                    },
                    _ => println!("Status not OK"),
                }

                Ok(())
            }


            SubCommand::Alerts(t) => {
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

                let delete_alert = t.delete;

                // prepare endpoints here and call get_helper for all and single
                if get_all_alerts {
                    let endpoint = format!("{}/data/{}/alerts", url, data_id);
                    res = self.get_helper(endpoint, tokenfile.clone())?;
                    if delete_alert {
                        println!("TODO create endpoint DEL /alerts/:id for each :id");
                    }
                }

                if get_single_alert {
                    let endpoint = format!("{}/alerts/{}", url, alert_id);
                    res = self.get_helper(endpoint, tokenfile.clone())?;
                    if delete_alert {
                        println!("TODO create endpoint DEL /alerts/:id");
                    }
                }

                match &res["status"][..] {
                    "success" => {
                        // let alerts = serde_json::from_str::<Vec<HashMap<String, String>>>(res["message"].as_str()).unwrap();
                        let alerts = serde_json::from_str::<Vec<AlertResponse>>(res["message"].as_str()).unwrap();
                        // println!("DBG DBG {:?} len={}", alerts, alerts.len());

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
                                    let res = self.del_helper(endpoint, tokenfile.clone());
                                }

                            }

                        }
                    },
                    _ => println!("Cannot retrieve alert(s). Status not OK"),
                }

                Ok(())
            },

            SubCommand::Set => unimplemented!(),

            // SubCommand::Publish => {
            //     unimplemented!()
            // },


            SubCommand::Profile(t) => {

                let input_to_fetch = &t.input;
                let publish_to_api = t.publish;
                println!("Publish after profile: {:?}", publish_to_api);

                let input_location: String = input_to_fetch.chars().skip(0).take(5).collect();

                match input_location.as_ref() {
                    // try s3 bucket
                    "s3://" => {
                        // TODO integrate DataFrame
                        let filename: String = input_to_fetch.chars().skip(5).collect();
                        println!("filename input_to_fetch: {}", filename);
                        let storage = Storage::new();
                        // TODO get this from input_to_fetch
                        // let filename = String::from("synthetic_demo_data.csv");
                        let df = self.csv_reader_helper(storage, filename);
                        let _profile = df.profile();
                        // println!("Dataset profile: {}", profile);
                    }

                    // try local file
                    _ => {
                        // input_to_fetch is a local file
                        let file = File::open(input_to_fetch.clone()).expect("could not read file");

                        let df = CsvReader::new(file)
                            .infer_schema(None)
                            .has_header(true)
                            .finish()
                            .unwrap();

                        // dbg!(&df);
                        let dataframe = NcodeDataFrame {
                            dataframe: Arc::new(df),
                        };

                        // TODO Result(profile)
                        let mut profile = dataframe.profile();

                        // add filename to profile
                        profile.set_datasource(input_to_fetch.clone());
                        // Convert to string and print
                        // let profile_str = serde_json::to_string_pretty(&profile).unwrap();
                        let profile_str = serde_json::to_value(&profile).unwrap();
                        // println!("Profile: {}", profile_str);

                        if publish_to_api {
                            // post profile to new url
                            let post_data_endpoint = format!("{}/data/", url);
                            let data_body = json!({"type": "local", "filename": format!("{}", input_to_fetch.clone()) });
                            // println!("DBG body: {:?}", data_body);
                            // println!("DBG body.to_string(): {:?}", data_body.to_string());

                            let res: HashMap<String, String> = self
                                .post_helper(post_data_endpoint, tokenfile.clone(), data_body)
                                .unwrap();

                            println!("DBG POST req res: {:?}", &res);
                            // println!("data_id: {:?}", res.get("data_id"));

                            // get data_id from response
                            match res.get("data_id") {
                                Some(did) => {
                                    println!("DBG in match did: {}", did);

                                    let post_profile_endpoint = format!("{}/data/{}/profile", url, did);
                                    let profile_res = self
                                         .post_helper(post_profile_endpoint, tokenfile.clone(), json!(profile_str))
                                         .unwrap();

                                    println!("DBG profile_res {:?}", &profile_res);

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

    fn get_helper(&self, endpoint: String, tokenfile: String) -> Result<HashMap<String, String>> {
        // get token and send to request as is (encoding occurs server-side)
        let token = get_content_from_file(&tokenfile[..])?;
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

    fn del_helper(&self, endpoint: String, tokenfile: String) -> Result<HashMap<String, String>> {
        // get token and send to request as is (encoding occurs server-side)
        let token = get_content_from_file(&tokenfile[..])?;
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

    fn post_helper(&self, endpoint: String, tokenfile: String, body: Value) -> Result<HashMap<String, String>> {
        let mut result: HashMap<String, String> = HashMap::new();
        let token = get_content_from_file(&tokenfile[..])?;
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
                    // println!("response: {}", response);
                    // let assets: String = res.text()?;
                    result.insert("status".to_string(), "success".to_string());
                    result.insert("message".to_string(), response.clone());

                } else if res.status() == reqwest::StatusCode::CONFLICT {
                    result = res.json()?;

                } else if res.status() == reqwest::StatusCode::CREATED {
                    result = res.json()?;
                }
            }

            Err(e) => {
                println!("Could not make request! {:?} ", e);
            }
        }

        Ok(result)
    }
}
