# Json2bin

A fast Jsonl to binidx files converter written in Rust.

## Usage
```
json2bin -h
Json converter to RWKV binidx file format
Usage: json2bin [OPTIONS] --input <INPUT>

Options:
  -i, --input <INPUT>            Jsonlines file to read
  -o, --output-dir <OUTPUT_DIR>  Output directory for binidx files [default: -]
  -h, --help                     Print help
  -V, --version                  Print version
```
Following command will convert the jsonl file src/sample.jsonl into src/sample.bin and src/sample.idx files.
```
json2bin -i src/sample.jsonl
```
The output directory can be set with the argument "--output-dir <OUTPUT_DIR>"

## Performance comparison
We compared the performance between this Rust json2bin and the Python json2binidx tool. 
