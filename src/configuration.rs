use std::fs::File;
use std::io::prelude::*;
use std::io::Error;
use toml;
use serde_derive::Deserialize;


#[derive(Deserialize, Debug)]
pub struct Config {
    pub api: Api,
    pub credentials: Credentials,
    pub settings: Settings

}

#[derive(Deserialize, Debug)]
pub struct Api {
    pub server: String,
    pub port: u16,
}

#[derive(Deserialize, Debug)]
pub struct Credentials {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Debug)]
pub struct Settings {
    pub tokenfile: String,
    // TODO other settings here
}


pub fn get_content_from_file(filepath: &str) -> Result<String, Error> {
    let mut file = File::open(filepath)?;
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    Ok(content)
}

pub fn get_configuration_from_file(filepath: &str) -> Result<Config, Error> {
    let configuration = get_content_from_file(filepath)?;
    let config: Config = toml::from_str(configuration.as_str()).unwrap();

    Ok(
        Config {
            api: Api {
                server: config.api.server,
                port: config.api.port,
            },

            credentials: Credentials {
                username: config.credentials.username,
                password: config.credentials.password
            },

            settings: Settings {
                tokenfile: config.settings.tokenfile
            }
        }
    )
}
