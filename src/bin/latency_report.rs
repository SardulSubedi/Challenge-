use anyhow::{Context, Result};
use clap::Parser;
use engineering_challenge::latency::{Analyzer, Observation};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

#[derive(Parser)]
#[command(
    about = "Compare receive times for identical updates across redundant sockets. JSON report on stdout."
)]
struct Args {
    input: PathBuf,
    #[arg(long, default_value_t = 100000)]
    match_window: usize,
}
fn main() -> Result<()> {
    let args = Args::parse();
    anyhow::ensure!(args.match_window > 0, "match-window must be positive");
    let mut analyzer = Analyzer::new(args.match_window);
    for (index, line) in BufReader::new(File::open(args.input)?).lines().enumerate() {
        let observation: Observation = serde_json::from_str(&line?)
            .with_context(|| format!("invalid observation on line {}", index + 1))?;
        analyzer.push(observation);
    }
    println!("{}", serde_json::to_string_pretty(&analyzer.finish())?);
    Ok(())
}
