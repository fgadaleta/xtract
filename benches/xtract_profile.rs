use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use serde_json::{ Value, json };
use std::fs::File;
use std::sync::Arc;
use polars::prelude::*;

use xtract::loaders::dataframe::NcodeDataFrame;

fn xtract_profile(c: &mut Criterion) {
    // input_to_fetch is a local file
    let file = File::open("./data/track_and_trace.csv")
        .expect("could not read file");

    let df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .finish().unwrap();

    let dataframe = NcodeDataFrame { dataframe: Arc::new(df) };
    let profile = dataframe.profile();
    // Convert to string and print
    let profile_str = serde_json::to_string_pretty(&profile).unwrap();
    println!("Profile: {}", profile_str);

}

criterion_group!(benches, xtract_profile);
criterion_main!(benches);