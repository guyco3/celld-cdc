//! A bucket plus key prefix over `object_store`, with the conditional writes
//! the cursor store needs.

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, ObjectMeta, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion,
};
use std::sync::Arc;

/// S3 connection settings. Empty strings fall back to the AWS environment.
#[derive(Clone, Debug, Default)]
pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Clone)]
pub struct Bucket {
    store: Arc<dyn ObjectStore>,
    /// The key prefix, empty or ending in `/`.
    prefix: String,
}

/// An object's content with the version token a conditional write needs.
pub struct Versioned {
    pub bytes: Bytes,
    pub e_tag: Option<String>,
}

/// The result of a conditional write.
#[derive(Debug, PartialEq, Eq)]
pub enum CasOutcome {
    Written { e_tag: Option<String> },
    Conflict,
}

impl Bucket {
    /// Open `s3://NAME[/PREFIX]`, or `file:///path[/PREFIX]` for tests.
    pub fn open(url: &str, s3: &S3Config) -> Result<Self> {
        if let Some(rest) = url.strip_prefix("s3://") {
            let (name, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            let mut b = AmazonS3Builder::from_env()
                .with_bucket_name(name)
                .with_conditional_put(S3ConditionalPut::ETagMatch);
            if !s3.region.is_empty() {
                b = b.with_region(&s3.region);
            }
            if !s3.access_key_id.is_empty() {
                b = b
                    .with_access_key_id(&s3.access_key_id)
                    .with_secret_access_key(&s3.secret_access_key);
            }
            if !s3.endpoint.is_empty() {
                b = b
                    .with_endpoint(&s3.endpoint)
                    .with_virtual_hosted_style_request(false)
                    .with_allow_http(s3.endpoint.starts_with("http://"));
            }
            let store = b.build().with_context(|| format!("open {url}"))?;
            return Ok(Self::new(Arc::new(store), prefix));
        }
        if let Some(path) = url.strip_prefix("file://") {
            std::fs::create_dir_all(path)?;
            let store = object_store::local::LocalFileSystem::new_with_prefix(path)?;
            return Ok(Self::new(Arc::new(store), ""));
        }
        anyhow::bail!("unsupported bucket url {url}; use s3:// or file://")
    }

    /// An in-memory bucket, for tests.
    pub fn memory() -> Self {
        Self::new(Arc::new(object_store::memory::InMemory::new()), "")
    }

    pub fn new(store: Arc<dyn ObjectStore>, prefix: &str) -> Self {
        let mut prefix = prefix.trim_matches('/').to_string();
        if !prefix.is_empty() {
            prefix.push('/');
        }
        Self { store, prefix }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    fn path(&self, key: &str) -> Path {
        Path::from(format!("{}{key}", self.prefix))
    }

    /// Strip the bucket prefix from a listed location.
    fn relative(&self, location: &Path) -> String {
        let s = location.as_ref();
        s.strip_prefix(&self.prefix).unwrap_or(s).to_string()
    }

    pub async fn get(&self, key: &str) -> Result<Option<Versioned>> {
        match self.store.get_opts(&self.path(key), GetOptions::default()).await {
            Ok(r) => {
                let e_tag = r.meta.e_tag.clone();
                Ok(Some(Versioned {
                    bytes: r.bytes().await?,
                    e_tag,
                }))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e).with_context(|| format!("get {key}")),
        }
    }

    pub async fn get_bytes(&self, key: &str) -> Result<Bytes> {
        self.get(key)
            .await?
            .map(|v| v.bytes)
            .with_context(|| format!("{key} not found"))
    }

    pub async fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Bytes> {
        let opts = GetOptions {
            range: Some(GetRange::Bounded(range)),
            ..Default::default()
        };
        let r = self
            .store
            .get_opts(&self.path(key), opts)
            .await
            .with_context(|| format!("get range of {key}"))?;
        Ok(r.bytes().await?)
    }

    pub async fn put(&self, key: &str, bytes: Bytes) -> Result<()> {
        self.store
            .put(&self.path(key), PutPayload::from_bytes(bytes))
            .await
            .with_context(|| format!("put {key}"))?;
        Ok(())
    }

    /// Create `key` if absent (`e_tag` None) or replace the version `e_tag`.
    pub async fn put_if(&self, key: &str, bytes: Bytes, e_tag: Option<&str>) -> Result<CasOutcome> {
        let mode = match e_tag {
            None => PutMode::Create,
            Some(tag) => PutMode::Update(UpdateVersion {
                e_tag: Some(tag.to_string()),
                version: None,
            }),
        };
        let opts = PutOptions {
            mode,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&self.path(key), PutPayload::from_bytes(bytes), opts)
            .await
        {
            Ok(r) => Ok(CasOutcome::Written { e_tag: r.e_tag }),
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. })
            // `If-Match` against a missing key is a 404 on S3.
            | Err(object_store::Error::NotFound { .. }) => Ok(CasOutcome::Conflict),
            Err(e) => Err(e).with_context(|| format!("conditional put {key}")),
        }
    }

    /// Every object under `prefix`, with keys relative to the bucket prefix.
    pub async fn list(&self, prefix: &str) -> Result<Vec<(String, ObjectMeta)>> {
        let path = self.path(prefix);
        let metas: Vec<ObjectMeta> = self
            .store
            .list(Some(&path))
            .try_collect()
            .await
            .with_context(|| format!("list {prefix}"))?;
        Ok(metas
            .into_iter()
            .map(|m| (self.relative(&m.location), m))
            .collect())
    }

    /// The immediate child "directories" of `prefix`, relative keys ending
    /// in `/`.
    pub async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>> {
        let path = self.path(prefix);
        let r = self
            .store
            .list_with_delimiter(Some(&path))
            .await
            .with_context(|| format!("list dirs {prefix}"))?;
        Ok(r
            .common_prefixes
            .iter()
            .map(|p| format!("{}/", self.relative(p)))
            .collect())
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        match self.store.delete(&self.path(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e).with_context(|| format!("delete {key}")),
        }
    }
}
