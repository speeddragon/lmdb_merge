use anyhow::Result;
use clap::{Parser, Subcommand};
use heed::types::Bytes;
use heed::{Database, EnvOpenOptions};
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Merge multiple LMDB databases into one
    Merge {
        /// Output LMDB directory
        #[arg(short, long)]
        output: PathBuf,

        /// Input LMDB directories
        #[arg(required = true)]
        inputs: Vec<PathBuf>,
    },
    /// Count keys in LMDB databases
    Count {
        /// Input LMDB directories
        #[arg(required = true)]
        inputs: Vec<PathBuf>,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();

    match args.command {
        Command::Merge { output, inputs } => merge(&output, &inputs),
        Command::Count { inputs } => count(&inputs),
    }
}

fn merge(output: &PathBuf, inputs: &[PathBuf]) -> Result<()> {
    std::fs::create_dir_all(output)?;
    let out_env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1000 * 1000 * 16384 * 64) // 10TB - 1MB (16 KB*64)
            .max_dbs(10)
            .open(output)?
    };

    let mut out_txn = out_env.write_txn()?;
    let out_db: Database<Bytes, Bytes> = out_env.create_database(&mut out_txn, None)?;

    for input_path in inputs {
        println!("Processing {:?}", input_path);

        let in_env = unsafe { EnvOpenOptions::new().max_dbs(10).open(input_path)? };

        let in_txn = in_env.read_txn()?;
        let in_db: Database<Bytes, Bytes> = in_env
            .open_database(&in_txn, None)?
            .expect("Database not found");

        let mut counter = 0;

        for result in in_db.iter(&in_txn)? {
            let (key, value) = result?;
            out_db.put(&mut out_txn, key, value)?;
            counter += 1;

            if counter % 1_000_000 == 0 {
                out_txn.commit()?;
                out_txn = out_env.write_txn()?;
            }
        }
    }

    out_txn.commit()?;
    println!("Merge completed.");

    Ok(())
}

fn count(inputs: &[PathBuf]) -> Result<()> {
    let mut total: u64 = 0;

    for input_path in inputs {
        let in_env = unsafe { EnvOpenOptions::new().max_dbs(10).open(input_path)? };

        let in_txn = in_env.read_txn()?;
        let in_db: Database<Bytes, Bytes> = in_env
            .open_database(&in_txn, None)?
            .expect("Database not found");

        let count = in_db.len(&in_txn)?;
        println!("{:?}: {} keys", input_path, count);
        total += count;
    }

    println!("Total: {} keys", total);

    Ok(())
}
