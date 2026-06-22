use super::StorageBackend;
use async_trait::async_trait;
use aws_sdk_s3::Client;

pub struct S3Backend {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3Backend {
    pub async fn new(
        bucket: String,
        region: String,
        prefix: Option<String>,
        endpoint: Option<String>,
    ) -> anyhow::Result<Self> {
        let mut config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(region));

        if let Some(ep) = endpoint {
            config_builder = config_builder.endpoint_url(ep);
        }

        let config = config_builder.load().await;
        let client = Client::new(&config);

        Ok(Self {
            client,
            bucket,
            prefix: prefix.unwrap_or_default(),
        })
    }

    fn s3_key(&self, sha256: &str) -> String {
        if self.prefix.is_empty() {
            sha256.to_string()
        } else {
            format!("{}/{}", self.prefix, sha256)
        }
    }
}

#[async_trait]
impl StorageBackend for S3Backend {
    async fn put(&self, sha256: &str, data: &[u8]) -> anyhow::Result<u64> {
        let len = data.len() as u64;
        let key = self.s3_key(sha256);
        let body = aws_sdk_s3::primitives::ByteStream::from(data.to_vec());
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body)
            .send()
            .await?;
        Ok(len)
    }

    async fn get(&self, sha256: &str) -> anyhow::Result<Vec<u8>> {
        let key = self.s3_key(sha256);
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await?;
        let data = output.body.collect().await?;
        Ok(data.into_bytes().to_vec())
    }

    async fn exists(&self, sha256: &str) -> anyhow::Result<bool> {
        let key = self.s3_key(sha256);
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn delete(&self, sha256: &str) -> anyhow::Result<()> {
        let key = self.s3_key(sha256);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await?;
        Ok(())
    }
}
