use polars::prelude::*;
use std::sync::Arc;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};
use regex::Regex;
use lazy_static;

pub enum Type {
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
}

#[derive(Default)]
pub struct Column {
    name: String,
    hash: Vec<u8>,
    nunique: usize,
    count: usize,
    null_count: usize,
    // categorical: bool,
    types: Vec<Type>
}


impl Column {
    /// Create new metadata for column
    pub fn new(name: String, hash: Vec<u8>, nunique: usize,
               count: usize, null_count: usize, types: Vec<Type>) -> Self {

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
        assert!(threshold > 0f64);

        let ratio = self.nunique as f64 / self.count as f64;
        if ratio > threshold { false }
        else { true }
    }

    pub fn set_hash(&mut self, hash: Vec<u8>) {
        self.hash = hash;
    }

//     pub fn get_types(&self) -> Vec<DataType> {
//         vec![]
//     }

//     pub fn get_data_info(&self) {

//     }
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
            println!("{:?}", &coltype);
            coltypes.push(coltype);
            let mut hasher = DefaultHasher::new();
            let mut parsed_types: Vec<Type> = vec![];

            match coltype {
                DataType::Int64 => {
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

                    // let _something: Series = colvalues
                    // .sort(false).f64()
                    // .expect("series was not an f64 dtype")
                    // .into_iter()
                    // .map(|opt_elem| opt_elem.map(|elem|
                    //     {
                    //         let num_str = elem.to_ne_bytes();
                    //         hasher.write(&num_str);
                    //         elem
                    //     }
                    // ))
                    // .collect();
                },
                // generic string
                DataType::Utf8 => {
                    // if inferred type is string,
                    // try parse each element into known regex
                    let re = Regex::new(r"(\d{4})-(\d{2})-(\d{2})").unwrap();

                    let coliter = colvalues
                    .utf8()
                    .expect("something")
                    .into_iter();

                    for element in coliter {
                        match element {
                            Some(el) => {
                                let is_iban = re.is_match(el);
                                if is_iban { parsed_types.push(Type::Iban); }

                                let elem_str = el.as_bytes();
                                hasher.write(&elem_str);
                            },
                            _ => panic!("Some element is wrong")
                        }
                    }

                },
                _ => unimplemented!()
            }

            let colhash = hasher.finish().to_string();
            println!("column hash: {}", colhash);


            let null_count = colvalues.null_count();
            // get number of unique values
            let nunique = colvalues.unique().unwrap().len();

            // TODO calculate hash of colvalues after sorting
            // TODO

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