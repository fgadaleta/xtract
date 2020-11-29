## Developer commands from `cargo`

### Login

`cargo run -- login`


### Profile file given as input
WIP

`cargo profile --input /path/to/file.csv`

`xtract profile --input data/user_transactions_small.csv`

## Profile file stored in s3 bucket

`xtract profile -i s3://bucket_name/filename.csv`
`xtract profile -i s3://synthetic_demo_data.csv`

## Profile file stored in  local filesystem

`xtract profile -i ./data/filename.csv`
