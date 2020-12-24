use super::cli::{Args, SubCommand};
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use serde::{Serialize, Deserialize};
use serde::de::{
    DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess,
    VariantAccess, Visitor,
};
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::Cursor;
use std::sync::Arc;
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

                // let get_all = t.all?;
                let get_all = match t.all {
                    Some(opt) => opt,
                    None => false,
                };

                if get_all {
                    let endpoint = format!("{}/data", url);
                    res = self.get_helper(endpoint, tokenfile)?;

                } else {
                    // fetching only one asset with id
                    // let id_to_fetch = t.id.as_ref().unwrap();
                    let id_to_fetch = match t.id.as_ref() {
                        Some(id) => String::from(id),
                        None => String::from(""),
                    };

                    let endpoint = format!("{}/data/{}", url, id_to_fetch);
                    res = self.get_helper(endpoint, tokenfile)?;
                }


                // let var = serde_json::from_string::<Vec<HashMap<String, String>>(input);
                // let assets: HashMap<String, Value> = serde_json::from_str(&assets[..]).unwrap();
                // let assets: HashMap<String, Value> = serde_json::from_str(&res["message"][..]).unwrap();
                // println!("Assets: {:?}", assets);
                // if res["status"] == "success" {}


                /*
                let array: Vec<String> = serde_json::from_str(res["message"].as_str())?;
                for elem in array.iter() {
                    println!("single data asset {:?}", elem);
                }
                 */

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
                            // println!("profile: {}", asset["profile_id"]);
                        }

                        // println!("Data assets: {}", res["message"])
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

                // TODO prepare endpoints here and call get_helper
                if get_all_alerts {
                    let endpoint = format!("{}/data/{}/alerts", url, data_id);
                    res = self.get_helper(endpoint, tokenfile.clone())?;
                    // println!("DBG alert res: {:?}", res);
                    // println!("data_id={:?} delete={:?}", data_id, delete_alert);
                    if delete_alert {
                        println!("TODO create endpoint DEL /alerts/:id for each :id");
                    }
                }

                if get_single_alert {
                    let endpoint = format!("{}/alerts/{}", url, alert_id);
                    res = self.get_helper(endpoint, tokenfile.clone())?;
                    // println!("DBG alert res: {:?}", res);
                    // println!("alert_id={:?} delete={:?}", alert_id, delete_alert);
                    if delete_alert {
                        println!("TODO create endpoint DEL /alerts/:id");
                    }
                }

                match &res["status"][..] {
                    "success" => {
                        // TODO serde_json deserialize with Option<fields>

                        // let alerts = serde_json::from_str::<Vec<HashMap<String, String>>>(res["message"].as_str()).unwrap();
                        let alerts = serde_json::from_str::<Vec<AlertResponse>>(res["message"].as_str());
                        // println!("DBG DBG {:?}", alerts);
                        for (i, alert) in alerts.iter().enumerate() {
                            println!("\n********** ALERT {} ********** ", i);
                            println!("data_id: {}", alert[0].data_id);
                            println!("created_on: {}", alert[0].created_on);
                            println!("alert_id: {}", alert[0].id);
                            let body = &alert[0].body;
                            println!("timestamp: {}", &body.timestamp);
                            println!("alert_type: {}", &body.r#type);
                            let messages: &Vec<String> = &body.message;
                            for (j, message) in messages.iter().enumerate() {
                                println!("\n____________________ msg {} ____________________ \n", j);
                                println!("    {}", message);
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
                let input_to_fetch = match t.input.as_ref() {
                    Some(input) => String::from(input),
                    None => String::from(""),
                };

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
                        let mut profile = dataframe.profile();
                        // add filename to profile
                        profile.set_datasource(input_to_fetch.clone());

                        // Convert to string and print
                        let _profile_str = serde_json::to_string_pretty(&profile).unwrap();
                        // println!("Profile: {}", profile_str);

                        // TODO
                        if publish_to_api {
                            // TODO add datasource as local filename first
                            // TODO get data_id from response
                            // TODO post profile to new url

                            let post_data_endpoint = format!("{}/data/", url);
                            let data_body = json!({"type": "local",
                            "filename": "synthetic_demo_data.csv",
                            });

                            println!("DBG body: {:?}", data_body);
                            println!("DBG body.to_string(): {:?}", data_body.to_string());

                            let res = self
                                .post_helper(post_data_endpoint, tokenfile.clone(), data_body)
                                .unwrap();
                            println!("DBG POST req res: {:?}", res);

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
                }

                Ok(result)
            }

            Err(e) => {
                println!("Could not make the request! (Oops..)");
                Err(e)
            }
        }
    }

    fn get_helper(&self, endpoint: String, tokenfile: String) -> Result<HashMap<String, String>> {
        // let endpoint = format!("{}/data", url);

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
                    // println!("Assets: {}", assets);
                    // response = assets;
                    result.insert("status".to_string(), "success".to_string());
                    result.insert("message".to_string(), str_assets.clone());

                    // println!("str_assets: {}", str_assets);

                    // print directly from here
                    // let assets: HashMap<String, Value> = serde_json::from_str(&str_assets[..]).unwrap();

                    /*
                    for (hash, info) in &assets {
                        println!("\n\n----------------------------\n");
                        println!("Data hash: [{}]\n", hash);
                        println!("----------------------------\n");
                        println!("{}", serde_json::to_string_pretty(&info).unwrap());
                        println!("\n------------------------\n");
                    }
                     */

                } else {
                    println!("Status not ok");
                    // response = res.text()?;
                    result.insert("status".to_string(), "failed".to_string());
                    result.insert("message".to_string(), res.text()?);
                    // println!("Response: {:?}", response);
                }
            }

            Err(e) => {
                println!("Could not make request! {:?} ", e);
            }
        }

        Ok(result)
    }

    fn post_helper(&self, endpoint: String, tokenfile: String, body: Value) -> Result<()> {
        // let gist_body = json!({
        //     "description": "the description for this gist",
        //     "public": true,
        //     "files": {
        //          "main.rs": {
        //          "content": r#"fn main() { println!("hello world!");}"#
        //         }
        //     }});

        let token = get_content_from_file(&tokenfile[..])?;
        dbg!("get_content_from_file token: {}", &token);
        // let token = base64::encode(token);
        let http_client = reqwest::blocking::ClientBuilder::new().build()?;
        let response = http_client
            .post(&endpoint)
            .header("Authorization", format!("{}{}", "Bearer ", token))
            .json(&body)
            .send();

        match response {
            Ok(res) => {
                if res.status() == reqwest::StatusCode::OK {
                    let response: String = res.json()?;
                    println!("response: {}", response);
                // let assets: String = res.text()?;
                } else if res.status() == reqwest::StatusCode::CONFLICT {
                    println!("Data asset already submitted. Still ok...");
                    let response: String = res.text()?;
                    let response: HashMap<String, String> =
                        serde_json::from_str(&response[..]).unwrap();

                    println!("response: {:?}", response);
                }
            }

            Err(e) => {
                println!("Could not make request! {:?} ", e);
            }
        }

        Ok(())
    }
}
