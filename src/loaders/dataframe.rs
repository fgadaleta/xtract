use polars::prelude::*;
use std::sync::Arc;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::collections::HashMap;
use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};
use regex::Regex;

use crate::parsers::iban::validate_iban;

#[derive(Hash, Eq, PartialEq, Debug)]
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
    Unknown
}

#[derive(Debug)]
pub struct NumericColumnFeatures {
    min: f64,
    max: f64,
    mean: f64,
    variance: f64,
    std: f64,
    // TODO histogram
}

impl NumericColumnFeatures {

    fn get_numeric_features(data: &Series) -> Self {
        let max: f64 = data.max().unwrap();
        let min: f64 = data.min().unwrap();
        let mean: f64 = data.mean().unwrap();
        let std: f64  = data.std_as_series().sum().unwrap();
        let variance: f64  = data.var_as_series().sum().unwrap();

        Self{
            min,
            max,
            mean,
            variance,
            std
        }
    }

}

#[derive(Debug)]
pub struct StringColumnFeatures {
    min_len: usize,
    max_len: usize,
    avg_len: f64,
    n_capitalized: usize,
    n_lowercase: usize,
    n_uppercase: usize
}


#[derive(Default)]
pub struct Column {
    name: String,
    hash: Vec<u8>,
    nunique: usize,
    count: usize,
    null_count: usize,
    types: Vec<ColumnType>
}


impl Column {
    /// Create new metadata for column
    pub fn new(name: String, hash: Vec<u8>, nunique: usize,
               count: usize, null_count: usize, types: Vec<ColumnType>) -> Self {

        Column {
            name,
            hash,
            nunique,
            count,
            null_count,
            types
            }
    }

    pub fn is_categorical(&self, threshold: f64) -> bool {
        assert!(self.count > 0);

        let ratio = self.nunique as f64 / self.count as f64;
        ratio < threshold
    }

    pub fn set_hash(&mut self, hash: Vec<u8>) {
        self.hash = hash;
    }

}


fn get_string_features(data: &Series) -> StringColumnFeatures {
    // WIP
    StringColumnFeatures {
        min_len: 0,
        max_len: 0,
        avg_len: 0f64,
        n_capitalized: 0,
        n_lowercase: 0,
        n_uppercase: 0
    }
}


pub struct NcodeDataFrame {
    pub dataframe: Arc<DataFrame>,
    // columns metadata
}

impl NcodeDataFrame {

    pub fn profile(&self) -> String {
        let mut columns: Vec<Column> = vec![];

        let (nrows, ncols) = self.dataframe.shape();
        let colnames = self.dataframe.get_column_names();
        let mut coltypes: Vec<&DataType> = vec![];

        for colname in colnames {
            // extract values of this column
            let colvalues = self.dataframe.column(colname).unwrap();
            // extract inferred column type
            let coltype = colvalues.dtype();
            // println!("{:?}", &coltype);
            coltypes.push(coltype);
            let mut hasher = DefaultHasher::new();
            let mut parsed_types: HashMap<ColumnType, u32> = HashMap::new();

            match coltype {
                DataType::Int64 => {
                    let numeric_features = NumericColumnFeatures::get_numeric_features(&colvalues);
                    // let numeric_features = get_numeric_features(&colvalues);
                    println!("num_feats: {:?}", &numeric_features);

                    let coliter = colvalues
                                        .i64()
                                        .expect("something")
                                        .into_iter();

                    for element in coliter {
                        match element {
                            Some(el) => {
                                let num_str = el.to_ne_bytes();
                                hasher.write(&num_str);
                            },
                            _ => panic!("Some element is wrong")
                        }
                    }
                },

                DataType::Float64 => {
                    // TODO compute mean, std, max,min
                    let numeric_features = NumericColumnFeatures::get_numeric_features(&colvalues);
                    // let numeric_features = get_numeric_features(&colvalues);
                    println!("num_feats: {:?}", &numeric_features);

                    let coliter = colvalues
                    .f64()
                    .expect("something")
                    .into_iter();

                    for element in coliter {
                        match element {
                            Some(el) => {
                                let num_str = el.to_ne_bytes();
                                hasher.write(&num_str);
                            },
                            _ => panic!("Some element is wrong")
                        }
                    }
                },
                // generic string
                DataType::Utf8 => {
                    // if inferred type is string, try parse each element into known regex
                    let regex_email_address = Regex::new(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)").unwrap();

                    let coliter = colvalues
                    .utf8()
                    .expect("Something wrong happened converting element to string")
                    .into_iter();

                    let mut total_len: usize = 0;

                    for element in coliter {
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
                                 }

                                else {
                                    *parsed_types.entry(ColumnType::Unknown).or_insert(0) += 1;

                                }
                                let elem_str = el.as_bytes();
                                hasher.write(&elem_str);

                                // add len of single element
                                total_len += elem_str.len();
                            },
                            _ => panic!("Some element is wrong")
                        }
                    }
                    let mean_element_len = total_len as f64 / nrows as f64;
                },

                _ => unimplemented!()
            }

            let colhash = hasher.finish().to_string();
            println!("column hash: {}", colhash);
            println!("parsed_types: {:?}", parsed_types);

            let null_count = colvalues.null_count();
            // get number of unique values
            let nunique = colvalues.unique().unwrap().len();

            let col = Column::new(colname.to_string(),
                                        vec![],
                                        nunique,
                                        nrows,
                                        null_count,
                                        vec![]);
            columns.push(col);

        }

        // TODO columns of string type are scanned element-wise to infer complex types


        String::from("")
    }

}