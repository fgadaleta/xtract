// https://github.com/ritchie46/polars/blob/master/examples/iris_classifier/src/main.rs

use polars::prelude::*;
use crate::loaders::dataframe::DataFrameMeta;


pub struct Transformer {
    profile: DataFrameMeta,
    dataframe: DataFrame
}

impl Transformer {
    pub fn new(dataframe: DataFrame, profile: DataFrameMeta) -> Self {
        Self {
            profile,
            dataframe
        }
    }

    pub fn transform(&self) -> DataFrame {

        // assert dataframe is consistent with profile from colnames
        let data_cols = self.dataframe.get_column_names();
        let profile_cols = self.profile.get_column_names();

        for col in data_cols.clone() {
            let col = col.to_string();
            let exists = profile_cols.iter().any(|i| i == &col.clone());
            assert!(exists, "Data and profile do not match");
        }

        // perform column transformation using profile
        let mut transformed_cols: Vec<Series> = vec![];
        for col in data_cols.clone() {
            let colvalues = self.dataframe.column(col).unwrap().to_owned();
            transformed_cols.push(colvalues);
        }

        let df = DataFrame::new(transformed_cols).unwrap();
        df
    }
}