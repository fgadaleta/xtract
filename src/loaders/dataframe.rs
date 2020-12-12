use arrow::datatypes::DataType;
use histo_fp::Histogram;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;
// use rayon::prelude::*;

use crate::parsers::iban::validate_iban;

/// Struct for JSON serialization
///
#[derive(Serialize, Deserialize)]
pub struct DataFrameMeta {
    datasource: String,
    hash: String,
    profile: ProfileMeta,
}

impl DataFrameMeta {
    pub fn get_column_names(&self) -> Vec<String> {
        self.profile.columns.keys().cloned().collect()
    }

    pub fn set_datasource(&mut self, name: String) {
        self.datasource = name.clone();

        let mut hasher = DefaultHasher::new();
        hasher.write(name.as_bytes());
        self.profile.data_id = hasher.finish().to_string();
    }

    pub fn data_id(&self) -> String {
        self.profile.data_id.to_owned()
    }
}

#[derive(Serialize, Deserialize)]
pub struct ProfileMeta {
    data_id: String,
    nrows: usize,
    ncols: usize,
    columns: HashMap<String, Column>,
}

#[derive(Serialize, Deserialize)]
pub struct Column {
    hash: String,
    nunique: usize,
    count: usize,
    null_count: usize,
    categorical: bool,
    features: ColumnFeatures,
    types: HashMap<ColumnType, usize>,
}

#[derive(Hash, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum ColumnType {
    Str,
    Float,
    Int,
    Email,
    Iban,
    Currency,
    Address,
    Location,
    PersonName,
    Nan,
    Unknown,
}

#[derive(Serialize, Deserialize)]
pub enum ColumnFeatures {
    Numeric(NumericFeatures),
    String(StringFeatures),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Hist {
    bins: Vec<f64>,
    counts: Vec<u64>,
}

impl Hist {
    pub fn new() -> Self {
        Self {
            bins: vec![],
            counts: vec![],
        }
    }

    pub fn update(&mut self, bins: Vec<f64>, counts: Vec<u64>) {
        self.bins = bins;
        self.counts = counts;
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NumericFeatures {
    min: f64,
    max: f64,
    mean: f64,
    variance: f64,
    std: f64,
    hist: Option<Hist>,
}

impl NumericFeatures {
    fn get_numeric_features(data: &Series) -> Self {
        let max: f64 = data.max().unwrap();
        let min: f64 = data.min().unwrap();
        let mean: f64 = data.mean().unwrap();
        let std: f64 = data.std_as_series().sum().unwrap();
        let variance: f64 = data.var_as_series().sum().unwrap();

        Self {
            min,
            max,
            mean,
            variance,
            std,
            hist: Some(Hist::new()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StringFeatures {
    min_len: usize,
    max_len: usize,
    avg_len: f64,
    n_capitalized: usize,
    n_lowercase: usize,
    n_uppercase: usize,
}

impl StringFeatures {
    fn get_string_features(_data: &Series) -> Self {
        // TODO WIP

        Self {
            min_len: 0,
            max_len: 0,
            avg_len: 0f64,
            n_capitalized: 0,
            n_lowercase: 0,
            n_uppercase: 0,
        }
    }
}

impl Column {
    /// Create new metadata for column
    pub fn new(
        hash: String,
        nunique: usize,
        count: usize,
        null_count: usize,
        features: ColumnFeatures,
        types: HashMap<ColumnType, usize>,
    ) -> Self {
        assert!(count > 0);
        let ratio = nunique as f64 / count as f64;
        const THRESHOLD: f64 = 0.2;

        Column {
            // name,
            hash,
            nunique,
            count,
            null_count,
            categorical: ratio < THRESHOLD,
            features,
            types,
        }
    }

    /// Set categorical flag wrt to threshold
    ///
    pub fn set_categorical(&mut self, threshold: f64) {
        assert!(self.count > 0);

        let ratio = self.nunique as f64 / self.count as f64;
        self.categorical = ratio < threshold;
    }

    pub fn set_hash(&mut self, hash: String) {
        self.hash = hash;
    }
}

pub struct NcodeDataFrame {
    pub dataframe: Arc<DataFrame>,
}

impl NcodeDataFrame {
    pub fn profile(&self) -> DataFrameMeta {
        let (nrows, ncols) = self.dataframe.shape();
        let colnames = self.dataframe.get_column_names();
        let mut coltypes: Vec<&DataType> = vec![];
        // meta data for single column
        let mut columns_meta: HashMap<String, Column> = HashMap::new();

        for (_i, colname) in colnames.iter().enumerate() {
            // extract values of this column
            let colvalues = self.dataframe.column(colname).unwrap();
            // extract inferred column type
            let coltype = colvalues.dtype();
            coltypes.push(coltype);
            let mut hasher = DefaultHasher::new();
            let mut parsed_types: HashMap<ColumnType, usize> = HashMap::new();
            let colfeats: ColumnFeatures;
            let pb = ProgressBar::new(nrows as u64);
            let prefix = format!("Column: {}\t\t", colname);
            let s = ("Fade in: ", "█▉▊▋▌▍▎▏  ", "yellow");
            pb.set_style(
                ProgressStyle::default_bar()
                    // .template("{prefix:.bold} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
                    .template(&format!("{{prefix:.bold}}▕ {{bar:.{}}} ▏{{msg}}", s.2))
                    .progress_chars("##-"),
            );
            pb.set_prefix(&prefix[..]);
            pb.set_position(0);

            match coltype {
                DataType::Int64 => {
                    let mut histogram = Histogram::with_buckets(10, None);
                    let mut j = 0;
                    colvalues
                        .i64()
                        .expect("Something wrong happened reading column")
                        .into_iter()
                        .for_each(|element| {
                            match element {
                                Some(el) => {
                                    histogram.add(el as f64);
                                    let num_str = el.to_ne_bytes();
                                    hasher.write(&num_str);
                                    // update progress bar
                                    pb.inc(1);
                                    pb.set_message(&format!("{:3}%", 100 * j / nrows));
                                    j += 1;
                                }

                                _ => panic!("Something wrong happened during conversion"),
                            }
                        });

                    let mut bins: Vec<f64> = vec![];
                    let mut counts: Vec<u64> = vec![];

                    for bucket in histogram.buckets() {
                        bins.push(bucket.start());
                        counts.push(bucket.count());
                    }

                    let hist = Hist { bins, counts };
                    let mut numeric_features = NumericFeatures::get_numeric_features(&colvalues);
                    numeric_features.hist = Some(hist);
                    // colfeats = ColumnFeatures::Numeric{features: numeric_features};
                    colfeats = ColumnFeatures::Numeric(numeric_features);
                }

                DataType::Float64 => {
                    let mut histogram = Histogram::with_buckets(10, None);
                    let mut j = 0;
                    colvalues
                        .f64()
                        .expect("Something wrong happened reading column")
                        .into_iter()
                        .for_each(|element| {
                            match element {
                                Some(el) => {
                                    // count into histogram
                                    histogram.add(el);
                                    let num_str = el.to_ne_bytes();
                                    hasher.write(&num_str);
                                    // update progress bar
                                    pb.inc(1);
                                    pb.set_message(&format!("{:3}%", 100 * j / nrows));
                                    j += 1;
                                }

                                _ => panic!("Something wrong happened during conversion"),
                            }
                        });

                    let mut bins: Vec<f64> = vec![];
                    let mut counts: Vec<u64> = vec![];
                    for bucket in histogram.buckets() {
                        bins.push(bucket.start());
                        counts.push(bucket.count());
                    }
                    let hist = Hist { bins, counts };
                    let mut numeric_features = NumericFeatures::get_numeric_features(&colvalues);
                    numeric_features.hist = Some(hist);
                    // colfeats = ColumnFeatures::Numeric{features: numeric_features};
                    colfeats = ColumnFeatures::Numeric(numeric_features);
                }

                DataType::Utf8 => {
                    // if inferred type is string, try parse each element into known regex
                    let regex_email_address =
                        Regex::new(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)").unwrap();

                    // TODO
                    // get_string_features(&colvalues) and move parsing into get_string_features
                    let string_features = StringFeatures::get_string_features(&colvalues);
                    // colfeats = ColumnFeatures::String{features: string_features};
                    colfeats = ColumnFeatures::String(string_features);

                    let mut total_len: usize = 0;
                    let mut j = 0;
                    colvalues
                        .utf8()
                        .expect("Something wrong happened reading column")
                        .into_iter()
                        .for_each(|element| {
                            match element {
                                Some(el) => {
                                    let is_email = regex_email_address.is_match(el);
                                    let is_iban = validate_iban(el);
                                    if is_email {
                                        *parsed_types.entry(ColumnType::Email).or_insert(0) += 1;
                                    }
                                    // TODO all types here
                                    else if is_iban {
                                        *parsed_types.entry(ColumnType::Iban).or_insert(0) += 1;
                                    } else {
                                        *parsed_types.entry(ColumnType::Unknown).or_insert(0) += 1;
                                    }
                                    let elem_str = el.as_bytes();
                                    hasher.write(&elem_str);
                                    // add len of single element
                                    total_len += elem_str.len();
                                    // update progress bar
                                    pb.inc(1);
                                    pb.set_message(&format!("{:3}%", 100 * j / nrows));
                                    j += 1;
                                }

                                _ => panic!("Something wrong happened during conversion"),
                            }
                        });

                    let _mean_element_len = total_len as f64 / nrows as f64;
                }

                _ => unimplemented!(),
            }

            let colhash = hasher.finish().to_string();
            let null_count = colvalues.null_count();
            // get number of unique values
            let nunique = colvalues.unique().unwrap().len();

            let col = Column::new(colhash, nunique, nrows, null_count, colfeats, parsed_types);

            columns_meta.insert(colname.to_string(), col);
            pb.finish_with_message("done");
        }

        let profilemeta = ProfileMeta {
            data_id: String::from(""),
            nrows,
            ncols,
            columns: columns_meta,
        };

        let dfmeta = DataFrameMeta {
            datasource: String::from(""),
            // TODO hash of all sorted columns hashes
            hash: String::from("TODO"),
            profile: profilemeta,
        };

        dfmeta
    }
}
