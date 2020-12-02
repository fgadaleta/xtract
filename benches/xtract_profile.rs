// use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use serde_json::{ Value, json };
use std::fs::File;
use std::sync::Arc;
use polars::prelude::*;

// use xtract::loaders::dataframe::NcodeDataFrame;
// extern crate xtract;

// extern crate loaders::dataframe::NcodeDataFrame;

// fn xtract_profile(c: &mut Criterion) {
//     // input_to_fetch is a local file
//     let file = File::open("./data/user_transactions_small.csv")
//         .expect("could not read file");

//     let df = CsvReader::new(file)
//         .infer_schema(None)
//         .has_header(true)
//         .finish().unwrap();

//     // let dataframe = NcodeDataFrame { dataframe: Arc::new(df) };
//     // let profile = dataframe.profile();
//     // println!("Profile: {}", profile);

// }

// criterion_group!(benches, xtract_profile);
// criterion_main!(benches);