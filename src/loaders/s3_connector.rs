use rusoto_core::{HttpClient, Region};
use rusoto_credential::StaticProvider;
use rusoto_s3::{GetObjectRequest, S3Client, S3 };
use tokio::io::AsyncReadExt;
// use std::str;
// use std::error::Error;


pub struct Storage {
    name: String,
    client: S3Client,
    // region: Region,
    // credentials: Credentials,
    bucket: String,
}

impl Storage {

    pub fn new() -> Self {

        let region = Region::Custom {
            name: "us-east-1".into(),
            endpoint: "http://localhost:9000".into(),
        };

        let bucket = "frankiebucket".to_string();

        let s3_client = S3Client::new_with(
            HttpClient::new().expect("failed to create request dispatcher"),
            StaticProvider::new(
                "frag".to_owned(),
                "supersecretkey".to_owned(),
                None,
                None,
            ),
            region,
        );

        Storage {
            name: "minio".into(),
            client: s3_client,
            bucket,
        }

    }

    pub async fn get_object(&self, filename: String) -> (Vec<u8>, String) {
        let get_req = GetObjectRequest {
            bucket: self.bucket.to_owned(),
            key: filename.to_owned(),
            ..Default::default()
        };

        let mut data = self.client.get_object(get_req).await.expect("Could not GET remote file! :(( ");
        dbg!(&data);
        let content_type = data.content_type.unwrap();
        let mut stream = data.body.unwrap().into_async_read();
        let mut body = Vec::new();
        stream.read_to_end(&mut body).await.unwrap();


        (body, content_type)
        // println!("remote data {:?}", data_str);
    }


}