use clap::{Parser, Subcommand};
use memo_pir::client::MemoPirClient;

#[derive(Parser)]
#[command(
    name = "memo-pir-cli",
    about = "Query the standalone Ironwood memo PIR POC"
)]
struct Cli {
    #[arg(long)]
    server: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Metadata,
    Query { position: u64 },
    Dummy,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let client = MemoPirClient::connect(&cli.server).await?;
    match cli.command {
        Command::Metadata => println!("{}", serde_json::to_string_pretty(client.metadata())?),
        Command::Query { position } => {
            let record = client.query_position(position).await?;
            println!("{}", hex::encode(record.as_bytes()));
        }
        Command::Dummy => {
            client.query_dummy().await?;
            println!("dummy query completed");
        }
    }
    Ok(())
}
