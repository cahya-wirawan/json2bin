use std::ffi::c_float;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use rwkv_tokenizer;
use serde::Deserialize;
use bytemuck::cast_slice;
use clap::Parser;
use tqdm::pbar;
use std::time::Instant;

const MAGIC_HDR: &str = "MMIDIDX\x00\x00";
const VERSION: [u8; 8] = [1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
const DTYPE: [u8; 1] = [8u8];
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
}


fn main() {
    println!("Json converter to RWKV binidx file format");
    let args = Args::parse();
    let tokenizer = rwkv_tokenizer::WorldTokenizer::new(None).unwrap();

    //let input = args.input.clone();
    let file_in = File::open(&args.input).expect("couldn't open file");
    let filename = &args.input.file_name().unwrap();
    let output_dir;
    output_dir = if args.output_dir.to_str().unwrap() == "-" {
        &args.input.parent().unwrap()
    } else {
        &*args.output_dir
    };
    let metadata = fs::metadata(&args.input);
    let filesize = metadata.unwrap().len();
    let mut file_bin = output_dir.join(filename);
    file_bin.set_extension("bin");
    let mut file_idx = output_dir.join(filename);
    file_idx.set_extension("idx");

    let file_bin = File::create(file_bin).expect("couldn't open file");
    let file_idx = File::create(file_idx).expect("couldn't open file");
    let mut bytes_counter = 0;
    let mut tokens_counter = 0;
    let mut doc_length: u64 = 0;
    let mut doc_sizes: Vec<u32> = Vec::new();
    let mut doc_pointers: Vec<u64> = vec![0u64];
    let mut doc_indexes: Vec<u64> = vec![0u64];
    let mut file_bin_writer = BufWriter::new(file_bin);
    let mut file_idx_writer = BufWriter::new(file_idx);
    let mut pbar = pbar(Some(filesize as usize));
    let start = Instant::now();
    for line in BufReader::new(file_in).lines() {
        doc_length += 1;
        let line = line.expect("couldn't get line");
        let line_length = line.len();
        pbar.update(line_length+1).unwrap();
        bytes_counter += line_length;
        let ds: Jsonline = serde_json::from_str(&line).unwrap();
        let mut token_ids = tokenizer.encode(ds.text.as_str());
        token_ids.push(0);
        let token_bytes: &[u8] = cast_slice(&token_ids);
        file_bin_writer.write(token_bytes).expect("Can't write");
        tokens_counter += token_ids.len();
        doc_sizes.push(token_ids.len() as u32);
        let last_pointer = doc_pointers[doc_pointers.len()-1];
        doc_pointers.push(last_pointer + 2*token_ids.len() as u64);
        doc_indexes.push(doc_indexes[doc_indexes.len()-1] + 1);
    }
    file_bin_writer.flush().unwrap();
    let elapsed = start.elapsed();

    doc_pointers.pop();
    file_idx_writer.write(MAGIC_HDR.as_bytes()).expect("Can't write");
    file_idx_writer.write(cast_slice(&VERSION)).expect("Can't write");
    file_idx_writer.write(cast_slice(&DTYPE)).expect("Can't write");
    file_idx_writer.write(cast_slice(&[doc_length])).expect("Can't write");
    file_idx_writer.write(cast_slice(&[doc_length+1])).expect("Can't write");
    file_idx_writer.write(cast_slice(&doc_sizes)).expect("Can't write");
    file_idx_writer.write(cast_slice(&doc_pointers)).expect("Can't write");
    file_idx_writer.write(cast_slice(&doc_indexes)).expect("Can't write");
    file_idx_writer.flush().unwrap();
    let mut filename_bin = filename.to_str().unwrap().replace(".jsonl", "").to_owned();
    filename_bin.push_str(".bin");
    let mut filename_idx = filename.to_str().unwrap().replace(".jsonl", "").to_owned();
    filename_idx.push_str(".idx");
    println!("- Output files:  {}/{{{filename_bin},{filename_idx}}}", output_dir.to_str().unwrap());
    println!("- Bytes read: {:?}", bytes_counter);
    println!("- Tokens written: {:?}", tokens_counter);
    println!("- Bytes/tokens: {:?}", bytes_counter as c_float/tokens_counter as c_float);
    println!("- Elapsed time: {:.2?}", elapsed);
    println!("- Performance: {:.2?}MB/s", bytes_counter as f32/elapsed.as_secs_f32()/(1024*1024) as f32);
}
