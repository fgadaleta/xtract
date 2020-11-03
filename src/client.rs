use crate::cli::{ Args, SubCommand};
use crate::configuration::{ get_content_from_file, get_configuration_from_file };
use serde_json::{ Value, json };
use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use crate::loaders::s3_connector::Storage;
use tokio::runtime::Runtime;
use once_cell::sync::Lazy;
use crate::loaders::csv_format::{ CsvReader };
use std::io::Cursor;
use arrow::record_batch::RecordBatch;
use crate::loaders::frame::DataFrame;
use arrow::datatypes::DataType;
// use arrow::csv::ReaderBuilder as ArrowReaderBuilder;
// use crate::loaders::csv_format::Loader;


// #[cfg(feature = "prettyprint")]
// use arrow::util::print_batches;

static RT: Lazy<Runtime> = Lazy::new(|| {
    Runtime::new().unwrap()
});


pub struct Frontend {
    args: Args,
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
            },

            SubCommand::Get(t) => {
                // let get_all = t.all?;
                let get_all = match t.all {
                    Some(opt) => opt,
                    None => false
                };

                if get_all {
                    let endpoint = format!("{}/data", url);
                    self.get_helper(endpoint, tokenfile)?;
                }
                else {
                    // fetching only one asset with id
                    // let id_to_fetch = t.id.as_ref().unwrap();
                    let id_to_fetch = match t.id.as_ref() {
                        Some(id) => String::from(id),
                        None => String::from("")
                    };

                    let endpoint = format!("{}/data/{}", url, id_to_fetch);
                    self.get_helper(endpoint, tokenfile)?;
                }

                Ok(())
            },

            SubCommand::Set => {
                unimplemented!()
            },

            SubCommand::Publish => {
                unimplemented!()
            },

            SubCommand::Profile(t) => {
                let input_to_fetch = match t.input.as_ref() {
                    Some(input) => String::from(input),
                    None => String::from("")
                };
                println!("input_to_fetch: {}", input_to_fetch);
                let storage = Storage::new();

                // TODO get this from input_to_fetch
                let filename = String::from("synthetic_demo_data.csv");

                let df = self.csv_reader_helper(storage, filename);
                let _profile = df.profile();

                Ok(())
            },


        }
    }

    fn csv_reader_helper(&self, storage: Storage, filename: String) -> DataFrame {
        let fut = async { storage.get_object(filename).await  };
        let (data, data_type) = RT.handle().block_on(fut);
        println!("content type: {}", data_type);

        let data_str = String::from_utf8(data).unwrap();
        let file = Cursor::new(&data_str[..]);
        // from here on it's like a regular file descriptor
        let mut reader = CsvReader::new(file)
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
                    None => { has_next = false; }
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

    fn login_helper(&self, url: String, payload: serde_json::Value) -> Result<String, reqwest::Error> {
        let endpoint = format!("{}/auth/login", url);
        let http_client = reqwest::blocking::ClientBuilder::new().build()?;
        let response = http_client
            .post(&endpoint)
            .json(&payload)
            .send();

        // println!("DEBUG {:?}", response);
        match response {
            Ok(res) => {
                let mut result = String::new();
                if res.status() == reqwest::StatusCode::OK {
                    let auth_token = res.json::<HashMap<String, String>>()?;
                    result = auth_token["Authorization"].to_string();

                }
                else {
                    println!("Response not 200 OK :(");
                }

                Ok(result)
            },

            Err(e) => {
                println!("Could not make the request! (Oops..)");
                Err(e)
            }

        }
    }

    fn get_helper(&self, endpoint: String, tokenfile: String) -> Result<()> {
        // let endpoint = format!("{}/data", url);
        let token = get_content_from_file(&tokenfile[..])?;
        let token = base64::encode(token);
        let http_client = reqwest::blocking::ClientBuilder::new().build()?;
        let response = http_client
            .get(&endpoint)
            .header("Authorization", format!("{}{}", "Bearer ", token))
            .send();

        match response {
            Ok(res) => {
                if res.status() == reqwest::StatusCode::OK {
                    println!("status ok ");
                    let assets: String = res.json()?;
                    // let assets: String = res.text()?;
                    println!("Assets: {}", assets);

                    let assets: HashMap<String, Value> = serde_json::from_str(&assets[..]).unwrap();
                    for (hash, info) in &assets {
                        println!("\n\n----------------------------\n");
                        println!("Data hash: [{}]\n", hash);
                        println!("----------------------------\n");
                        println!("{}", serde_json::to_string_pretty(&info).unwrap());
                        println!("\n------------------------\n");
                    }
                }
                else {
                    println!("Status not ok");
                }
            },

            Err(e) => {
                println!("Could not make request! {:?} ", e);
            }
        }

        Ok(())
    }

}

