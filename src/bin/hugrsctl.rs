use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = hugrs::hugrsctl_cli::Cli::parse();
    hugrs::hugrsctl_cli::run(cli).await
}
