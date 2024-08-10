use std::ffi::c_float;
use std::fs::{File, remove_file, rename};
use std::{cmp, thread};
use std::sync::mpsc;
use std::io::{BufRead, BufReader, BufWriter, Seek, Write, SeekFrom};
use std::sync::mpsc::Sender;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use clap::Parser;
use tqdm::pbar;
use serde::Deserialize;
use bytemuck::cast_slice;
use std::io::prelude::*;
use rwkv_tokenizer;

// const DEFAULT_THREADS_NUMBER: u16 = 8;
const MAGIC_HDR: &str = "MMIDIDX\x00\x00";
const VERSION: [u8; 8] = [1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
const DTYPE: [u8; 1] = [8u8];
const VEC_STEP: usize = 1024*1024;
#[derive(Deserialize, Debug)]
struct Jsonline {
    text: String
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Jsonlines file to read
    #[arg(short, long)]
    input: std::path::PathBuf,

    /// Output directory for binidx files
    #[arg(short, long, required = false, default_value = "-")]
    output_dir: std::path::PathBuf,

    /// Number of threads
    #[arg(short, long, required = false, default_value = "8")]
    thread: u16,

    /// Verbosity
    #[arg(short, long, action)]
    verbose: bool,
}

#[derive(Debug)]
struct Metadata {
    index: u16,
    doc_length: u64,
    doc_sizes: Vec<u32>,
    bytes_counter: usize,
    tokens_counter: usize,
    performance: f32
}


fn json2bin(thread_index: u16, max_threads: u16, tx: Sender<Metadata>, filename: &str) {
    let tokenizer = rwkv_tokenizer::WorldTokenizer::new(None).unwrap();
    let input  = PathBuf::from(filename);
    let input_ = input.clone();
    let output_dir = input_.parent().unwrap();
    let mut file_in = File::open(input.clone()).expect("couldn't open file");
    let filename = format!("{}_{}", input.file_stem().unwrap().to_str().unwrap(), thread_index);
    let file_path_bin = output_dir.join(format!("{filename}.bin"));
    let file_bin = File::create(file_path_bin).expect("couldn't open file");

    let mut doc_length: u64 = 0;
    let mut doc_sizes: Vec<u32> = Vec::new();
    let mut file_bin_writer = BufWriter::new(file_bin);

    let file_size = file_in.metadata().unwrap().len();
    let start_position = file_size/max_threads as u64 *thread_index as u64;
    let current_position = file_in.seek(SeekFrom::Start(start_position)).unwrap();
    assert_eq!(start_position, current_position);
    let mut line_counter = 0;
    let mut bytes_counter = 0;
    let mut tokens_counter = 0;
    let mut file_size_per_thread = file_size/max_threads as u64;
    let mut pbar = pbar(Some(file_size_per_thread as usize));
    let mut buf_reader = BufReader::new(file_in);
    let mut line = String::new();
    let start = Instant::now();
    loop {
        let byte_read: usize;
        if line_counter == 0 {
            let mut line_vec: Vec<u8> = Vec::new();
            let ret = buf_reader.read_until(0xA as u8, &mut line_vec);
            byte_read = ret.unwrap_or_else(|error| {
                print!("File read error: {:?}", error);
                0
            });
            if thread_index != 0 {
                if byte_read == 0 { break; } // eof
                let line_length = line_vec.len();
                if file_size_per_thread > line_length as u64 {
                    pbar.update(line_length).unwrap();
                    file_size_per_thread -= line_length as u64;
                } else {
                    pbar.update(file_size_per_thread as usize).unwrap();
                    file_size_per_thread = 0;
                }
                bytes_counter += line_length;
                line_counter += 1;
                continue;
            };
            line = String::from_utf8(line_vec).unwrap()
        } else {
            let ret = buf_reader.read_line(&mut line);
            byte_read = ret.unwrap_or_else(|error| {
                print!("File read error: {:?}", error);
                0
            });
        }
        if byte_read == 0 { break; } // eof
        let line_length = line.len();
        if file_size_per_thread > line_length as u64 {
            pbar.update(line_length).unwrap();
            file_size_per_thread -= line_length as u64;
        } else {
            pbar.update(file_size_per_thread as usize).unwrap();
            file_size_per_thread = 0;
        }
        bytes_counter += line_length;
        line_counter += 1;
        doc_length += 1;
        let ds: Jsonline = serde_json::from_str(&line).unwrap();
        let mut token_ids = tokenizer.encode(ds.text.as_str());
        token_ids.push(0);
        tokens_counter += token_ids.len();
        let token_bytes: &[u8] = cast_slice(&token_ids);
        file_bin_writer.write(token_bytes).expect("Can't write");
        doc_sizes.push(token_ids.len() as u32);
        if bytes_counter as u64 >= file_size/max_threads as u64 {
            break;
        }
        line.clear();
    }
    let elapsed = start.elapsed();
    let performance = bytes_counter as f32/elapsed.as_secs_f32()/(1024*1024) as f32;
    file_bin_writer.flush().unwrap();
    let metadata = Metadata {
        index: thread_index,
        doc_length: doc_length,
        doc_sizes: doc_sizes,
        bytes_counter: bytes_counter,
        tokens_counter: tokens_counter,
        performance: performance
    };
    tx.send(metadata).unwrap();
}

fn main() {
    let (tx, rx) = mpsc::channel();
    let mut results: HashMap<u16, Metadata> = HashMap::new();
    println!("Jsonl converter to RWKV binidx file format");
    let args = Args::parse();

    let root_filename = args.input.file_stem().unwrap().to_str().unwrap();
    let filename = args.input.to_str().unwrap();
    let output_dir;
    output_dir = if args.output_dir.to_str().unwrap() == "-" {
        &args.input.parent().unwrap()
    } else {
        &*args.output_dir
    };
    let threads_number = args.thread;
    let verbose = args.verbose;

    let start = Instant::now();
    thread::scope(|scope| {
        for thread_index in 0..threads_number {
            let tx_ = tx.clone();
            scope.spawn(move || {
                json2bin(thread_index, threads_number, tx_, filename);
            });
        }
    });
    for (index, metadata) in rx.iter().enumerate() {
        if verbose {
            println!("{}", format!("Thread {}: {:.2}MB/s", metadata.index, metadata.performance));
        }
        results.insert(metadata.index, metadata);
        if index as u16 >= (threads_number - 1) {
            break;
        }
    }
    println!("Merging binidx data.");
    let mut document_length_all = 0;
    let mut bytes_counter_all= 0;
    let mut tokens_counter_all= 0;
    for index in 0..threads_number {
        document_length_all += results[&index].doc_length;
        bytes_counter_all += results[&index].bytes_counter;
        tokens_counter_all += results[&index].tokens_counter;
    }

    let file_idx = output_dir.join(format!("{root_filename}.idx"));
    let file_idx_out = File::create(file_idx).expect("couldn't open file");
    let mut buf_idx_writer: BufWriter<File> = BufWriter::new(file_idx_out);
    buf_idx_writer.write(MAGIC_HDR.as_bytes()).expect("Can't write");
    buf_idx_writer.write(cast_slice(&VERSION)).expect("Can't write");
    buf_idx_writer.write(cast_slice(&DTYPE)).expect("Can't write");
    buf_idx_writer.write(cast_slice(&[document_length_all])).expect("Can't write");
    buf_idx_writer.write(cast_slice(&[document_length_all+1])).expect("Can't write");

    for index in 0..threads_number {
        for i in (0..results[&index].doc_sizes.len()).step_by(VEC_STEP) {
            buf_idx_writer.write(cast_slice(&results[&index].doc_sizes[i..cmp::min(i+VEC_STEP, results[&index].doc_sizes.len())])).expect("Can't write");
        }
    }
    buf_idx_writer.flush().unwrap();
    buf_idx_writer.write(cast_slice(&[0u64])).expect("Can't write");
    let mut last_pointer: u64 = 0;
    for index in 0..threads_number {
        for i in 0..results[&index].doc_sizes.len() {
            if (index == threads_number - 1) && (i == results[&index].doc_sizes.len() - 1) {
                break;
            }
            let pointer = last_pointer + 2 * results[&index].doc_sizes[i] as u64;
            buf_idx_writer.write(cast_slice(&[pointer])).expect("Can't write");
            last_pointer = pointer;
        }
    }
    for i in 0..document_length_all+1 {
        buf_idx_writer.write(cast_slice(&[i as u64])).expect("Can't write");
    }
    buf_idx_writer.flush().unwrap();

    let file_bin = output_dir.join(format!("{root_filename}_0.bin"));
    let file_bin_out = File::options().append(true)
        .open(file_bin.clone()).expect("couldn't open file");
    let mut buf_bin_writer: BufWriter<File>;
    buf_bin_writer = BufWriter::new(file_bin_out);
    for index in 0..threads_number {
        if index == 0 {
            continue;
        } else {
            let file_x_bin = output_dir.join(format!("{root_filename}_{index}.bin"));
            let file_bin_in = File::open(file_x_bin.clone()).expect("couldn't open file");
            let file_size = file_bin_in.metadata().unwrap().len();
            let buf_reader = BufReader::new(file_bin_in);
            let mut part_reader = buf_reader.take(file_size);
            std::io::copy(&mut part_reader, &mut buf_bin_writer).unwrap();
            buf_bin_writer.flush().unwrap();
            remove_file(file_x_bin).expect("couldn't remove file");
        }
    }
    rename(file_bin, output_dir.join(format!("{root_filename}.bin"))).unwrap();
    let elapsed = start.elapsed();
    println!("Results:");
    println!("- Output files:  {}/{{{root_filename}.bin,{root_filename}.idx}}", output_dir.to_str().unwrap());
    println!("- Bytes read: {:?} ({:.2?}MB)", bytes_counter_all, bytes_counter_all as f32/(1024*1024) as f32);
    println!("- Tokens written: {:?}", tokens_counter_all);
    println!("- Bytes/tokens: {:.2?}", bytes_counter_all as c_float/tokens_counter_all as c_float);
    println!("- Elapsed time: {:.2?}", elapsed);
    println!("- Performance: {:.2?}MB/s", bytes_counter_all as f32/elapsed.as_secs_f32()/(1024*1024) as f32);
}
