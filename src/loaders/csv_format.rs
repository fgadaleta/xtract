use lazy_static::lazy_static;
use regex::{Regex, RegexBuilder};
use std::io::{Read, Seek};
// use std::fmt;
// use std::collections::HashSet;
// use csv::ReaderBuilder;
use std::sync::Arc;
use arrow::csv::{ Reader, ReaderBuilder};
use arrow::datatypes::Schema;
// use arrow::record_batch::{RecordBatch, RecordBatchReader};
// use arrow::util::pretty;

// use crate::loaders::datatypes::DataType;


lazy_static! {
    static ref DECIMAL_RE: Regex = Regex::new(r"^-?(\d+\.\d+)$").unwrap();
    static ref INTEGER_RE: Regex = Regex::new(r"^-?(\d+)$").unwrap();
    static ref BOOLEAN_RE: Regex = RegexBuilder::new(r"^(true)$|^(false)$")
        .case_insensitive(true)
        .build()
        .unwrap();
}


// #[derive(Copy)]
pub struct CsvReader<R>
where R: Read + Seek
{
    /// stream object
    pub reader: R,
    /// Csv reader builder
    pub reader_builder: ReaderBuilder,

}

impl<R> CsvReader <R>
where R: Read + Seek
{
    pub fn new(reader: R) -> Self {
        CsvReader{
            reader,
            reader_builder: ReaderBuilder::new()
        }
    }

    pub fn with_schema(mut self, schema: Arc<Schema>) -> Self {
        self.reader_builder = self.reader_builder.with_schema(schema);
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.reader_builder = self.reader_builder.with_batch_size(batch_size);
        self
    }

    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.reader_builder = self.reader_builder.with_delimiter(delimiter);
        self
    }

    pub fn has_header(mut self, has_header: bool) -> Self {
        self.reader_builder = self.reader_builder.has_header(has_header);
        self
    }

    pub fn infer_schema(mut self, nrecords: usize) -> Self {
        self.reader_builder = self.reader_builder.infer_schema(Some(nrecords));
        // println!("Inferred schema: {:?}", schema);
        self

        // let mut rdr = self.reader_builder.from_reader(&data[..]);
        // let headers = self.reader_builder.has_headers(true);

        // // infer schema
        // let headers: Vec<String> = headers.iter().map(|s| s.to_string()).collect();
        // let header_length = headers.len();
        // println!("headers: {:?}\nlen: {}", headers, header_length);
    }

    pub fn finish(mut self) -> Reader<R> {
        self.reader_builder.build(self.reader).unwrap()
    }


    // pub fn get_column(mut self, colname: String) {
    //     let some = self.reader_builder.build(reader: R)

    // }
}




// /// Infer the data type of a record
// pub fn infer_field_schema(string: &str) -> DataType {
//     // when quoting is enabled in the reader, these quotes aren't escaped, we default to
//     // Utf8 for them
//     if string.starts_with('"') {
//         return DataType::Utf8;
//     }
//     // match regex in a particular order
//     if BOOLEAN_RE.is_match(string) {
//         DataType::Boolean
//     } else if DECIMAL_RE.is_match(string) {
//         DataType::Float64
//     } else if INTEGER_RE.is_match(string) {
//         DataType::Int64
//     } else {
//         DataType::Utf8
//     }
// }


// pub fn infer_schema(data: Vec<u8>) {

//     let mut rdr = ReaderBuilder::new().from_reader(&data[..]);
//     let headers = rdr.headers().unwrap();
//     // infer schema
//     let headers: Vec<String> = headers.iter().map(|s| s.to_string()).collect();
//     let header_length = headers.len();
//     println!("headers: {:?}\nlen: {}", headers, header_length);
//     // save position after reading headers
//     let position = rdr.position().clone();
//     println!("position after headers: {:?}", position);

//     // inferred field types
//     let mut column_types: Vec<HashSet<DataType>> = vec![HashSet::new(); header_length];
//     // columns with nulls
//     let mut nulls: Vec<bool> = vec![false; header_length];
//     // return to position after header
//     // rdr.seek(position).unwrap();
//     let mut records_count = 0;

//     for records in rdr.records().take(std::usize::MAX) {
//         let record = records.unwrap();
//         records_count += 1;
//         for i in 0..header_length {
//             if let Some(string) = record.get(i) {
//                 if string == "" {
//                     nulls[i] = true;
//                 } else {
//                     column_types[i].insert(infer_field_schema(string));
//                 }
//             }
//         }
//     }


//     println!("cols with null: {:?} ", nulls);
//     println!("col types: {:?} ", column_types);

//     // Ok()

// }