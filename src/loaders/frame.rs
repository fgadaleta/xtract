use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet};
use arrow::datatypes::{Field, Schema, DataType };
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use histo_fp::Histogram;
use noisy_float::prelude::*;
use serde_json;
use serde_json::Value;
use serde::{Deserialize, Serialize};
use crate::loaders::error::*;



/// Generic type that encapsulates vecs of primitive types
#[derive(Debug, Clone)]
pub enum GenericVector {
    I(Vec<i64>),
    F(Vec<R64>),
    S(Vec<String>),
}

impl GenericVector {
    fn len(&self) -> usize {
            match self {
                Self::I(v) => v.len(),
                Self::F(v) => v.len(),
                Self::S(v) => v.len()
            }
        }

}

#[derive(Clone)]
pub struct ChunkedArray {
    chunks: Vec<Arc<dyn Array>>,
    num_rows: usize,
    null_count: usize,
}

impl ChunkedArray {

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn null_count(&self) -> usize {
        self.null_count
    }

    pub fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    pub fn chunks(&self) -> &Vec<Arc<dyn Array>> {
        &self.chunks
    }

    pub fn from_arrays(arrays: Vec<Arc<dyn Array>>) -> Self {
        let mut num_rows = 0;
        let mut null_count = 0;
        let data_type = &arrays[0].data_type();

        arrays.iter().for_each(|array| {
            assert!(&array.data_type() == data_type);
            num_rows += array.len();
            null_count += array.null_count();
        });

        ChunkedArray {
            chunks: arrays,
            num_rows,
            null_count
        }
    }

}

#[derive(Clone)]
pub struct Column {
    data: ChunkedArray,
    field: Field,
}

impl Column {

    pub fn to_array(&self) -> Result<ArrayRef> {
        Ok(arrow::compute::concat(self.data().chunks())?)
    }

    pub fn from_arrays(arrays: Vec<Arc<dyn Array>>, field: arrow::datatypes::Field ) -> Self {
        for array in &arrays {
            assert!(array.data_type() == field.data_type());
        }
        Column {
            data: ChunkedArray::from_arrays(arrays),
            field
        }
    }

    pub fn name(&self) -> &str {
        self.field.name()
    }

    pub fn null_count(&self) -> usize {
        self.data().null_count()
    }

    pub fn num_rows(&self) -> usize {
        self.data().num_rows()
    }

    pub fn data(&self) -> &ChunkedArray {
        &self.data
    }

    pub fn data_type(&self) -> &DataType {
        self.field.data_type()
    }

    pub fn uniques(&self) -> Result<GenericVector> {
        let values = self.to_array().unwrap();
        match self.data_type() {
            DataType::Float64 => {
                let mut uniques = HashSet::new();
                let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..values.len() {
                    let value = values.value(i) as f64;
                    let value = r64(value);
                    uniques.insert(value);
                }
                let v: Vec<_> = uniques.into_iter().collect();
                let v = v.to_vec();
                Ok(GenericVector::F(v))
             },

            DataType::Utf8 => {
                let mut uniques: HashSet<String> = HashSet::new();
                // let values: Arc<dyn Array> = self.to_array().unwrap();
                let values = values.as_any().downcast_ref::<StringArray>().unwrap();

                for i in 0..values.len() {
                    let value: &str = values.value(i);
                    uniques.insert(value.to_string());
                }
                let v: Vec<String> = uniques.into_iter().collect();
                // let v = v.to_owned().to_vec();
                Ok(GenericVector::S(v))

            },

            DataType::Int64 => {
                let mut uniques = HashSet::new();
                let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..values.len() {
                    let value = values.value(i);
                    uniques.insert(value);
                }
                let v: Vec<_> = uniques.into_iter().collect();
                let v = v.to_vec();
                Ok(GenericVector::I(v))
        },

            _ => panic!("Datatype not supported for uniques.")
        }
    }

    pub fn nunique(&self) -> usize {
        let unique_values = self.uniques().unwrap();
        unique_values.len()
    }

    pub fn hist(&self, nbins: u64, density: bool) -> Histogram {
        let values = self.to_array().unwrap();
        let datatype = self.data_type();
        let histogram = Histogram::with_buckets(nbins, None);

        match self.data_type() {

            DataType::Int64 => {
                let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
                // Histogram makes sense only for numeric data
                let mut histogram = Histogram::with_buckets(nbins, None);
                for i in 0..values.len() {
                    histogram.add(values.value(i) as f64);
                }
                histogram

            },

            DataType::Float64 => {
                let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
                // Histogram makes sense only for numeric data
                let mut histogram = Histogram::with_buckets(nbins, None);

                for i in 0..values.len() {
                    let value: f64 = values.value(i);
                    histogram.add(value);
                }

                // Iterate over buckets and do stuff with their range and count.
                for bucket in histogram.buckets() {
                    println!("start:{} end:{} count:{}", bucket.start(), bucket.end(), bucket.count());
                    // bins.push(bucket.start().to_string());
                    // TODO add probablility here
                    // if density {
                    //     counters.push(bucket.count() as f64 / values.len() as f64);
                    // }
                    // else {
                    //     counters.push(bucket.count() as f64);
                    // }

                }
                histogram
            },

            _ => panic!("Unsupported type for histogram")
        }
    }

    pub fn is_categorical(&self, threshold: f64) -> bool {
        let ratio: f64 = self.nunique() as f64 / self.num_rows() as f64;
        ratio < threshold
    }
}


#[derive(Serialize, Deserialize)]
pub struct DataFrameMeta {
    datasource: String,
    hash: String,
    profile: ProfileMeta
}
#[derive(Serialize, Deserialize)]
pub struct ProfileMeta {
    nrows: usize,
    ncols: usize,
    columns: HashMap<String, ColumnMeta>,
}

#[derive(Serialize, Deserialize)]
pub struct ColumnMeta {
    hash: Vec<u8>,
    nunique: usize,
    count: usize,
    categorical: bool,
    types: Vec<DataType>,
}

pub struct DataFrame {
    schema: Arc<Schema>,
    columns: Vec<Column>,
}

impl DataFrame {
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Return meta-data (Field) of column with name
    ///
    pub fn column_by_name(&self, name: &str) -> &Field {
        let column_number = self.schema.column_with_name(name).expect("Column not found!");
        let column = self.schema.field(column_number.0);
        column
    }

    /// Return index of column with name
    ///
    pub fn column_index(&self, name: &str) -> usize {
        let column_number = self.schema.column_with_name(name).expect("Column not found!");
        column_number.0

    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_rows(&self) -> usize {
        self.columns[0].data().num_rows()
    }

    /// Return column at index i
    ///
    pub fn column(&self, i: usize) -> &Column {
        &self.columns[i]
    }

    /// Return vector of column names
    ///
    pub fn columns(&self) -> Vec<String> {
        let colnames: Vec<String> = self.schema().fields().clone().into_iter().map(|field| field.name().clone()).collect();
        colnames
    }

    /// Return vector of column types
    pub fn column_types(&self) -> Vec<DataType> {
        let coltypes: Vec<DataType> = self.schema().fields().clone().into_iter().map(|field| field.data_type().clone()).collect();
        coltypes
    }

    pub fn from_record_batches(schema: Arc<Schema>, record_batches: Vec<RecordBatch>) -> Self {
        if record_batches.is_empty() {
            panic!("Error. No record batches found");
        }

        let numcols = record_batches[0].num_columns();
        let mut arrays: Vec<Vec<Arc<dyn Array>>> = vec![vec![]; numcols];

        for batch in &record_batches {
            batch.columns().iter().enumerate().for_each(|(i, array)| {
                arrays[i].push(array.to_owned());
            });
        }
        let columns = arrays
        .iter()
        .enumerate()
        .map(|(i, array)| Column::from_arrays(array.to_owned(), schema.field(i).clone()))
        .collect();

        DataFrame {
            schema,
            columns
        }
    }

    /// profile dataframe
    /// Return json of metadata
    ///
    pub fn profile(&self) -> String {
        // let ncols = self.num_columns();
        let nrows = self.num_rows();
        let colnames = self.columns();
        let coltypes: Vec<DataType> = self.column_types();
        // meta data for single column
        let mut columns_meta: HashMap<String, ColumnMeta> = HashMap::new();

        // extract profile of each column
        for name in colnames.iter() {
            let colidx = self.column_index(name);
            let singlecol = self.column(colidx);
            let nunique = singlecol.nunique();
            let coltype: DataType = singlecol.data_type().clone();
            let categorical = singlecol.is_categorical(0.2);

            let colmeta = ColumnMeta{
                hash: vec![],
                nunique,
                count: nrows,
                categorical,
                types: vec![coltype]
            };
            columns_meta.insert(name.to_string(), colmeta);
        }

        let profilemeta = ProfileMeta {
            nrows,
            ncols: colnames.len(),
            columns: columns_meta
        };

        let dfmeta = DataFrameMeta{
            datasource: String::from(""),
            hash: String::from(""),
            profile: profilemeta
        };

        let profile_str = serde_json::to_string(&dfmeta).unwrap();
        profile_str
    }

}


#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use super::*;

    #[test]
    fn create_table_from_csv() {
    }
}