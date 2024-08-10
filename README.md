# Json2bin

[![Crates.io Version](https://img.shields.io/crates/v/json2bin.svg)](https://crates.io/crates/json2bin)
[![Crates.io Downloads](https://img.shields.io/crates/d/json2bin.svg)](https://crates.io/crates/json2bin)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache_2.0-blue.svg)](https://github.com/cahya-wirawan/json2bin/blob/main/LICENSE.txt)

A fast multithreading Jsonl converter to RWKV binidx files written in Rust.

![performance-multithreading](https://raw.githubusercontent.com/cahya-wirawan/json2bin/main/data/performance-multithreading.png)

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
  -t, --thread <THREAD>          Number of threads [default: 8]
  -v, --verbose                  Verbosity
  -h, --help                     Print help
  -V, --version                  Print version
```
Following command will convert the jsonl file src/sample.jsonl into src/sample.bin and src/sample.idx files.
```
$ json2bin -i src/sample.jsonl
```
The output directory can be set with the argument "--output-dir <OUTPUT_DIR>" or "-o <OUTPUT_DIR>"
```
$ json2bin -i src/sample.jsonl -o output
```
The default threads number is 8, it can be changed with the argument "--thread" or "-t"
```
$ json2bin -i src/sample.jsonl -t 4
```

## Performance comparison

We converted a 19GB English Wikipedia (20231101.en) in jsonl format to binidx format in M2 Apple machine. 
The Rust json2bin run with 7 threads, and it was 70 times faster than the Python json2binidx:
- The Python [json2binidx](https://github.com/Abel2076/json2binidx_tool): 1:01:45 or 5.13MB/s
- This Rust json2bin: 52.64s or 360.86MB/s
