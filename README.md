# Json2bin

A fast Jsonl converter to RWKV binidx files. It is written in Rust for performance reason.

## Installation

```
$ cargo install json2bin
```

## Usage

```
$ json2bin -h
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
$ json2bin -i src/sample.jsonl
```
The output directory can be set with the argument "--output-dir <OUTPUT_DIR>"

## Performance comparison

We converted a 213MB simple english wikipedia in jsonl format to binidx format in M2 Apple machine. The Rust json2bin
is more than 24 times faster than the Python json2binidx:
The Python json2binidx: 46.87s
This Rust json2bin: 1.92s

