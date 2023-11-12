use clap::Parser;
use datafusion::{
    prelude::{CsvReadOptions, SessionContext}, arrow::record_batch::RecordBatch,
};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Analysis file
    #[arg(short, long)]
    file: String,

    #[arg(short, long, default_value = " ")]
    delimiter: char,

    /// Prefix used to filter row in the analysis file
    // #[arg(long)]
    // prefix: Option<String>,

    /// Sql used to query
    #[arg(short, long)]
    query: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    let (file_name, file_extension) = {
        let path = std::path::Path::new(&args.file);
        (
            path.file_name().unwrap().to_str().unwrap(),
            path.extension().map(|s| s.to_str().unwrap()).unwrap_or(""),
        )
    };

    let ctx = SessionContext::new();

    let read_option = CsvReadOptions::new()
        .has_header(false)
        .file_extension(file_extension)
        .delimiter(args.delimiter as u8);

    ctx.register_csv(file_name, &args.file, read_option).await?;

    // create a plan
    let df = ctx.sql(&args.query).await?;

    // execute the plan
    let results: Vec<RecordBatch> = df.collect().await?;

    // format the results
    let pretty_results = arrow_cast::pretty::pretty_format_batches(results.as_slice())?.to_string();

    println!("{}", pretty_results);
    Ok(())
}
