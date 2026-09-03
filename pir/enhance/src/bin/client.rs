use clap::{Parser, Subcommand};
use enhance_pir::client::EnhancePirClient;

#[derive(Parser)]
#[command(
    name = "enhance-pir-cli",
    about = "Query the Ironwood Enhance PIR service"
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
    let client = EnhancePirClient::connect(&cli.server).await?;
    match cli.command {
        Command::Metadata => println!("{}", serde_json::to_string_pretty(client.generation())?),
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
