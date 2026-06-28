use clap::Parser;
use hugrs::daemon_cli::DaemonCli;

#[test]
fn test_daemon_cli_parses_server_and_config_flags() {
    let cli = DaemonCli::parse_from([
        "hugrs",
        "--config",
        "/tmp/hugrs.toml",
        "--server-host",
        "0.0.0.0",
        "--server-port",
        "8080",
        "--db-path",
        "/tmp/hugrs.db",
    ]);

    assert_eq!(cli.config_file.as_deref(), Some("/tmp/hugrs.toml"));
    assert_eq!(cli.server_host.as_deref(), Some("0.0.0.0"));
    assert_eq!(cli.server_port, Some(8080));
    assert_eq!(cli.db_path.as_deref(), Some("/tmp/hugrs.db"));
}

#[test]
fn test_daemon_cli_defaults_to_empty_overrides() {
    let cli = DaemonCli::parse_from(["hugrs"]);

    assert!(cli.config_file.is_none());
    assert!(cli.server_host.is_none());
    assert!(cli.server_port.is_none());
    assert!(cli.db_path.is_none());
}
