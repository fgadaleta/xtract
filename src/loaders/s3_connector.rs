// extern crate s3;
// use s3::bucket::Bucket;
// use s3::creds::Credentials;
// use s3::region::Region;
// use std::str;

use rusoto_core::{HttpClient, Region};
use rusoto_credential::StaticProvider;
use rusoto_s3::{PutObjectRequest, GetObjectRequest, S3Client, S3};




pub struct Storage {
    name: String,
    client: S3Client,
    // region: Region,
    // credentials: Credentials,
    bucket: String,
    location_supported: bool
}

impl Storage {

    pub fn new() -> Self {

        let region = Region::Custom {
            name: "us-east-1".into(),
            endpoint: "http://localhost:9000".into(),
        };

        let bucket = "frankiebucket".to_string();

        // let credentials = Credentials::from_env_specific(
        //         Some("ACCESS_KEY"),
        //         Some("SECRET_ACCESS_KEY"),
        //         None,
        //         None).unwrap();

        // dbg!("DBUG credentials: {}", &credentials);
        // dbg!("DBUG bucketname: {}", &bucket);


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
            // region,
            // credentials,
            bucket,
            location_supported: false
        }


    //     Storage {
    //         name: "minio".into(),
    //         region,
    //         credentials,
    //         bucket,
    //         location_supported: false
    //     }

    }

    pub async fn get_object(&self, filename: String) /* -> (Vec<u8>, u16) */ {

        let get_req = GetObjectRequest {
            bucket: self.bucket.to_owned(),
            key: filename.to_owned(),
            ..Default::default()
        };

        let data = self.client.get_object(get_req).await.expect("Could not GET remote file! :(( ");
        println!("remote data {:?}", data);

        // let bucket = Bucket::new(
        //     &self.bucket,
        //     self.region.to_owned(),
        //     self.credentials.to_owned()).unwrap();
        // dbg!("DBUG bucket: {}", &bucket);
        // let (data, code) = bucket.get_object_blocking(filename).unwrap();
        // println!("S3 Status: {}", code);

        // (data, code)
        // let data_str = String::from_str(str::from_utf8(&data).unwrap());



    }


}