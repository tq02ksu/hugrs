use crate::admin_client::AdminClient;
use crate::control::{
    DeleteResponse, FileListItem, FileListResponse, FileShowResponse, GcPreviewResponse,
    GcResultResponse, RepoListResponse, RepoShowResponse, ServiceStatsResponse,
    ServiceStatusResponse,
};
use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "hugrsctl")]
pub struct Cli {
    #[arg(long, global = true)]
    pub json: bool,

    #[arg(long, global = true)]
    pub source: Option<String>,

    #[arg(long, global = true)]
    pub endpoint: Option<String>,

    #[arg(long, global = true)]
    pub admin_token: Option<String>,

    #[command(subcommand)]
    pub resource: Resource,
}

#[derive(Debug, Subcommand)]
pub enum Resource {
    Service(ServiceArgs),
    #[command(alias = "repos")]
    Repo(ReposArgs),
    #[command(alias = "files")]
    File(FilesArgs),
}

#[derive(Debug, Args)]
pub struct ServiceArgs {
    #[command(subcommand)]
    pub command: Option<ServiceCommand>,
}

#[derive(Debug, Subcommand)]
pub enum ServiceCommand {
    Status,
    Stats,
    Gc {
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        batch_size: Option<usize>,
    },
}

#[derive(Debug, Args)]
pub struct ReposArgs {
    #[command(subcommand)]
    pub command: Option<ReposCommand>,
}

#[derive(Debug, Subcommand)]
pub enum ReposCommand {
    List,
    Show { repo: String },
    Delete { repo: String },
}

#[derive(Debug, Args)]
pub struct FilesArgs {
    #[command(subcommand)]
    pub command: Option<FilesCommand>,
}

#[derive(Debug, Subcommand)]
pub enum FilesCommand {
    List,
    Show {
        #[arg(long)]
        repo: String,
        #[arg(long)]
        file: String,
    },
    Delete {
        #[arg(long)]
        repo: String,
        #[arg(long)]
        file: String,
    },
}

pub async fn run(cli: Cli) -> anyhow::Result<()> {
    let client = AdminClient::discover(cli.endpoint, cli.admin_token)?;

    match cli.resource {
        Resource::Service(args) => match args.command.unwrap_or(ServiceCommand::Status) {
            ServiceCommand::Status => {
                let value = client.service_status().await?;
                print_service_status(cli.json, &value);
            }
            ServiceCommand::Stats => {
                let value = client.service_stats().await?;
                print_service_stats(cli.json, &value);
            }
            ServiceCommand::Gc {
                dry_run,
                batch_size,
            } => {
                if dry_run {
                    let value = client.service_gc_preview().await?;
                    print_gc_preview(cli.json, &value);
                } else {
                    let value = client.service_gc_execute(batch_size).await?;
                    print_gc_result(cli.json, &value);
                }
            }
        },
        Resource::Repo(args) => match args.command.unwrap_or(ReposCommand::List) {
            ReposCommand::List => {
                let value = client.repos_list(cli.source.as_deref()).await?;
                print_repos_list(cli.json, &value);
            }
            ReposCommand::Show { repo } => {
                let value = client.repos_show(&repo, cli.source.as_deref()).await?;
                print_repo_show(cli.json, &value);
            }
            ReposCommand::Delete { repo } => {
                let value = client.repos_delete(&repo, cli.source.as_deref()).await?;
                print_delete_response(cli.json, &value);
            }
        },
        Resource::File(args) => match args.command.unwrap_or(FilesCommand::List) {
            FilesCommand::List => {
                let value = client.files_list(cli.source.as_deref()).await?;
                print_files_list(cli.json, &value);
            }
            FilesCommand::Show { repo, file } => {
                let value = client
                    .files_show(&repo, &file, cli.source.as_deref())
                    .await?;
                print_file_show(cli.json, &value);
            }
            FilesCommand::Delete { repo, file } => {
                let value = client
                    .files_delete(&repo, &file, cli.source.as_deref())
                    .await?;
                print_delete_response(cli.json, &value);
            }
        },
    }

    Ok(())
}

fn print_service_status(json: bool, value: &ServiceStatusResponse) {
    if json {
        print_json(value);
        return;
    }

    let rows = [
        ("status", value.status.clone()),
        ("version", value.version.clone()),
        ("endpoint", value.endpoint.clone()),
        ("cache root", value.cache.root.clone()),
        ("db path", value.cache.db_path.clone()),
        (
            "max size",
            value
                .cache
                .max_size
                .map(format_bytes)
                .unwrap_or_else(|| "unlimited".to_string()),
        ),
        (
            "sources",
            format!(
                "hf={} ({})  ms={} ({})",
                enabled(value.sources.hf.enabled),
                value.sources.hf.endpoint,
                enabled(value.sources.ms.enabled),
                value.sources.ms.endpoint
            ),
        ),
        (
            "admin token",
            format!(
                "{} ({})",
                enabled(value.auth.admin_token_configured),
                value.auth.admin_token_file
            ),
        ),
    ];
    let width = rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0);
    for (label, value) in rows {
        println!("{label:<width$}: {value}");
    }
}

fn print_service_stats(json: bool, value: &ServiceStatsResponse) {
    if json {
        print_json(value);
        return;
    }

    let rows = [
        ("repos", value.repos.to_string()),
        ("files", value.files.to_string()),
        ("logical size", format_bytes_i64(value.logical_bytes)),
        ("stored size", format_bytes_i64(value.stored_bytes)),
        (
            "saved size",
            format!(
                "{} ({:.1}%)",
                format_bytes_i64(value.saved_bytes),
                value.saved_percent
            ),
        ),
        ("fetched", format_bytes(value.fetched_bytes)),
        ("served", format_bytes(value.served_bytes)),
    ];
    let width = rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0);
    for (label, value) in rows {
        println!("{label:<width$}: {value}");
    }
}

fn print_gc_preview(json: bool, value: &GcPreviewResponse) {
    if json {
        print_json(value);
        return;
    }

    let rows = [
        ("candidate chunks", value.candidate_chunks.to_string()),
        ("candidate bytes", format_bytes(value.candidate_bytes)),
    ];
    let width = rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0);
    for (label, value) in rows {
        println!("{label:<width$}: {value}");
    }
}

fn print_gc_result(json: bool, value: &GcResultResponse) {
    if json {
        print_json(value);
        return;
    }

    let rows = [
        ("deleted chunks", value.deleted_chunks.to_string()),
        ("reclaimed", format_bytes(value.reclaimed_bytes)),
        ("skipped chunks", value.skipped_chunks.to_string()),
    ];
    let width = rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0);
    for (label, value) in rows {
        println!("{label:<width$}: {value}");
    }
}

fn print_repos_list(json: bool, value: &RepoListResponse) {
    if json {
        print_json(value);
        return;
    }

    if value.items.is_empty() {
        println!("No repos.");
        return;
    }

    print_repo_table(&value.items.iter().map(repo_row).collect::<Vec<_>>());
    println!();
    println!("total: {}", value.total);
}

fn print_repo_show(json: bool, value: &RepoShowResponse) {
    if json {
        print_json(value);
        return;
    }

    println!("repo: {}", value.repo);
    println!("sources: {}", value.sources.join(","));
    println!("files: {}", value.files);
    println!("logical size: {}", format_bytes_i64(value.logical_bytes));
    println!("last accessed: {}", value.last_accessed);
    println!();

    if value.items.is_empty() {
        println!("No files.");
        return;
    }

    print_file_table(&value.items);
}

fn print_files_list(json: bool, value: &FileListResponse) {
    if json {
        print_json(value);
        return;
    }

    if value.items.is_empty() {
        println!("No files.");
        return;
    }

    print_file_table(&value.items);
    println!();
    println!("total: {}", value.total);
}

fn print_file_show(json: bool, value: &FileShowResponse) {
    if json {
        print_json(value);
        return;
    }

    println!("repo: {}", value.repo);
    println!("file: {}", value.file);
    println!("sources: {}", value.sources.join(","));
    println!("size: {}", format_bytes_i64(value.size));
    println!(
        "content-type: {}",
        value.content_type.as_deref().unwrap_or("-")
    );
    println!("last accessed: {}", value.last_accessed);
}

fn print_delete_response(json: bool, value: &DeleteResponse) {
    if json {
        print_json(value);
        return;
    }

    println!("deleted: {}", yes_no(value.deleted));
    println!("deleted files: {}", value.deleted_files);
    println!("sources: {}", join_sources(&value.sources));
}

fn print_json<T: serde::Serialize>(value: &T) {
    println!("{}", serde_json::to_string_pretty(value).unwrap());
}

fn print_repo_table(rows: &[RepoRow]) {
    let repo_w = column_width(
        rows.iter()
            .map(|r| r.repo.as_str())
            .chain(std::iter::once("REPO")),
    );
    let source_w = column_width(
        rows.iter()
            .map(|r| r.sources.as_str())
            .chain(std::iter::once("SOURCES")),
    );
    let files_w = column_width(
        rows.iter()
            .map(|r| r.files.as_str())
            .chain(std::iter::once("FILES")),
    );
    let size_w = column_width(
        rows.iter()
            .map(|r| r.logical_size.as_str())
            .chain(std::iter::once("LOGICAL SIZE")),
    );

    println!(
        "{:<repo_w$}  {:<source_w$}  {:>files_w$}  {:>size_w$}  LAST ACCESSED",
        "REPO", "SOURCES", "FILES", "LOGICAL SIZE",
    );
    for row in rows {
        println!(
            "{:<repo_w$}  {:<source_w$}  {:>files_w$}  {:>size_w$}  {}",
            row.repo, row.sources, row.files, row.logical_size, row.last_accessed,
        );
    }
}

fn print_file_table(items: &[FileListItem]) {
    let repo_w = column_width(
        items
            .iter()
            .map(|i| i.repo.as_str())
            .chain(std::iter::once("REPO")),
    );
    let file_w = column_width(
        items
            .iter()
            .map(|i| i.file.as_str())
            .chain(std::iter::once("FILE")),
    );
    let source_w = column_width(
        items
            .iter()
            .map(|i| join_sources(&i.sources))
            .collect::<Vec<_>>()
            .iter()
            .map(|s| s.as_str())
            .chain(std::iter::once("SOURCES")),
    );
    let size_values = items
        .iter()
        .map(|i| format_bytes_i64(i.size))
        .collect::<Vec<_>>();
    let size_w = column_width(
        size_values
            .iter()
            .map(|s| s.as_str())
            .chain(std::iter::once("SIZE")),
    );

    println!(
        "{:<repo_w$}  {:<file_w$}  {:<source_w$}  {:>size_w$}  CONTENT-TYPE  LAST ACCESSED",
        "REPO", "FILE", "SOURCES", "SIZE",
    );
    for (item, size) in items.iter().zip(size_values.iter()) {
        println!(
            "{:<repo_w$}  {:<file_w$}  {:<source_w$}  {:>size_w$}  {}  {}",
            item.repo,
            item.file,
            join_sources(&item.sources),
            size,
            item.content_type.as_deref().unwrap_or("-"),
            item.last_accessed,
        );
    }
}

fn enabled(value: bool) -> &'static str {
    if value {
        "enabled"
    } else {
        "disabled"
    }
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn join_sources(sources: &[String]) -> String {
    if sources.is_empty() {
        "-".to_string()
    } else {
        sources.join(",")
    }
}

fn format_bytes_i64(bytes: i64) -> String {
    if bytes < 0 {
        bytes.to_string()
    } else {
        format_bytes(bytes as u64)
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn column_width<'a>(values: impl Iterator<Item = &'a str>) -> usize {
    values.map(str::len).max().unwrap_or(0)
}

fn repo_row(item: &crate::control::RepoListItem) -> RepoRow {
    RepoRow {
        repo: item.repo.clone(),
        sources: join_sources(&item.sources),
        files: item.files.to_string(),
        logical_size: format_bytes_i64(item.logical_bytes),
        last_accessed: item.last_accessed.clone(),
    }
}

struct RepoRow {
    repo: String,
    sources: String,
    files: String,
    logical_size: String,
    last_accessed: String,
}
