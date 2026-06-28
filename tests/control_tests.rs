use clap::Parser;
use hugrs::hugrsctl_cli::{Cli, FilesCommand, ReposCommand, Resource, ServiceCommand};

#[test]
fn test_service_defaults_to_status() {
    let cli = Cli::parse_from(["hugrsctl", "service"]);
    match cli.resource {
        Resource::Service(args) => {
            assert!(matches!(
                args.command.unwrap_or(ServiceCommand::Status),
                ServiceCommand::Status
            ));
        }
        _ => panic!("expected service resource"),
    }
}

#[test]
fn test_repos_defaults_to_list() {
    let cli = Cli::parse_from(["hugrsctl", "repo"]);
    match cli.resource {
        Resource::Repo(args) => {
            assert!(matches!(
                args.command.unwrap_or(ReposCommand::List),
                ReposCommand::List
            ));
        }
        _ => panic!("expected repo resource"),
    }
}

#[test]
fn test_repos_alias_maps_to_repo() {
    let cli = Cli::parse_from(["hugrsctl", "repos"]);
    match cli.resource {
        Resource::Repo(args) => {
            assert!(matches!(
                args.command.unwrap_or(ReposCommand::List),
                ReposCommand::List
            ));
        }
        _ => panic!("expected repo resource"),
    }
}

#[test]
fn test_files_show_requires_repo_and_file() {
    let cli = Cli::try_parse_from(["hugrsctl", "file", "show", "--repo", "repo-a"]);
    assert!(cli.is_err());

    let cli = Cli::parse_from([
        "hugrsctl",
        "file",
        "show",
        "--repo",
        "repo-a",
        "--file",
        "model.bin",
    ]);
    match cli.resource {
        Resource::File(args) => match args.command.unwrap() {
            FilesCommand::Show { repo, file } => {
                assert_eq!(repo, "repo-a");
                assert_eq!(file, "model.bin");
            }
            _ => panic!("expected file show command"),
        },
        _ => panic!("expected file resource"),
    }
}

#[test]
fn test_files_alias_maps_to_file() {
    let cli = Cli::parse_from([
        "hugrsctl",
        "files",
        "show",
        "--repo",
        "repo-a",
        "--file",
        "model.bin",
    ]);
    match cli.resource {
        Resource::File(args) => match args.command.unwrap() {
            FilesCommand::Show { repo, file } => {
                assert_eq!(repo, "repo-a");
                assert_eq!(file, "model.bin");
            }
            _ => panic!("expected file show command"),
        },
        _ => panic!("expected file resource"),
    }
}
