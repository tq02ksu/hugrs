#![allow(clippy::unwrap_used, clippy::expect_used, clippy::result_large_err)]
use hugrs::config::{CliOverrides, Config};

#[test]
fn test_cli_overrides_env_and_file() {
    figment::Jail::expect_with(|jail| {
        jail.create_file(
            "hugrs.toml",
            r#"
[server]
host = "127.0.0.1"
port = 3000
"#,
        )?;
        jail.set_env("HUGRS_SERVER_HOST", "192.168.1.10");

        let config = Config::load(CliOverrides {
            config_file: Some(jail.directory().join("hugrs.toml").display().to_string()),
            server_host: Some("0.0.0.0".into()),
            server_port: Some(8080),
            ..Default::default()
        })
        .map_err(|err| err.to_string())?;

        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 8080);
        Ok(())
    });
}

#[test]
fn test_env_overrides_file() {
    figment::Jail::expect_with(|jail| {
        jail.create_file(
            "hugrs.toml",
            r#"
[server]
host = "127.0.0.1"
port = 3000
"#,
        )?;
        jail.set_env("HUGRS_SERVER_HOST", "192.168.1.10");
        jail.set_env("HUGRS_SERVER_PORT", "9090");

        let config = Config::load(CliOverrides {
            config_file: Some(jail.directory().join("hugrs.toml").display().to_string()),
            ..Default::default()
        })
        .map_err(|err| err.to_string())?;

        assert_eq!(config.server.host, "192.168.1.10");
        assert_eq!(config.server.port, 9090);
        Ok(())
    });
}

#[test]
fn test_dotenv_overrides_file() {
    figment::Jail::expect_with(|jail| {
        jail.create_file(
            "hugrs.toml",
            r#"
[server]
port = 3000
"#,
        )?;
        jail.create_file(".env", "HUGRS_SERVER_PORT=7070\n")?;

        let config = Config::load(CliOverrides {
            config_file: Some(jail.directory().join("hugrs.toml").display().to_string()),
            ..Default::default()
        })
        .map_err(|err| err.to_string())?;

        assert_eq!(config.server.port, 7070);
        Ok(())
    });
}

#[test]
fn test_missing_cli_prefetch_depth_does_not_override_file() {
    figment::Jail::expect_with(|jail| {
        jail.create_file(
            "hugrs.toml",
            r#"
[storage]
prefetch_depth = 6
"#,
        )?;

        let config = Config::load(CliOverrides {
            config_file: Some(jail.directory().join("hugrs.toml").display().to_string()),
            ..Default::default()
        })
        .map_err(|err| err.to_string())?;

        assert_eq!(config.storage.prefetch_depth, 6);
        Ok(())
    });
}
