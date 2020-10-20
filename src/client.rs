use crate::cli::{ Args, SubCommand};
use crate::configuration::{ get_content_from_file, get_configuration_from_file };
use serde_json::{ Value, json };
use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
// use std::str;
use crate::loaders::s3_connector::Storage;
use tokio::runtime::Runtime;
use once_cell::sync::Lazy;


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

        // create S3 connector
        let storage = Storage::new();
        // let (data,code)  =

        // RT.handle().block_on(run());

        // let fut = storage.get_object("synthetic_demo_data.csv".to_string());
        // let data = Runtime.block_on(fut);
        // let rt = Runtime::new().unwrap();
        // let data = rt.block_on(async { storage.get_object("synthetic_demo_data.csv".to_string()).await  } ) ;
        let fut = async { storage.get_object("synthetic_demo_data.csv".to_string()).await  };
        let data = RT.handle().block_on(fut);


        // dbg!("")

        // rt.enter(|| fut);
        // RT.handle().block_on(fut);

        // let data_str = str::from_utf8(&data).unwrap();
        // dbg!("S3 data: ", &data_str);
        // dbg!("S3 status: ", &code);


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
        }
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

    // async fn get_remote_object(&self) {
    //     // connect to S3 bucket
    //     // TODO get this from config
    //     let endpoint = String::from("http://minio:9000");
    //     let bucket_name = String::from("frankiebucket");
    //     let filename: &str = "uk_cities_with_headers.csv";
    //
    //     // {
    //     // "s3":
    //     // {
    //     //     "endpoint_url": "http://minio:9000",
    //     //     "aws_access_key_id": "frag",
    //     //     "aws_secret_access_key": "supersecretkey",
    //     //     "bucket": "frankiebucket",
    //     //     "key": "uk_cities_with_headers.csv"
    //     // }
    //     // }
    //     let s3_connector = S3CustomClient::new(endpoint, bucket_name);
    //     println!("{}", s3_connector);
    //     // let result = s3_connector::get_object(filename)
    //     //         .await
    //     //         .expect("Couldn't GET remote object");
    //     // println!("{:#?}", result);
    // }


}

