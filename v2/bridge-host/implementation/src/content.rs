use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Mutex,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use crate::BridgeAttachment;

pub const MAX_CONTENT_BYTES: usize = 8 * 1024 * 1024;

/// Raw package content. Bytes never appear in diagnostics or the public Boxology contract.
pub struct ContentUpload {
    pub bridge_id: String,
    pub external_event_id: String,
    pub media_type: String,
    pub name: Option<String>,
    pub bytes: Vec<u8>,
}

/// Host-owned content metadata returned after durable storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredContent {
    pub attachment: BridgeAttachment,
    pub size: u64,
    pub sha256: String,
}

/// Content failures carry no payload bytes or filesystem details.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContentStoreError {
    Unavailable,
    UnknownHandle,
    InvalidContent,
}

#[async_trait]
pub trait ContentStore: Send + Sync {
    async fn put(&self, request: ContentUpload) -> Result<StoredContent, ContentStoreError>;
    async fn owns(
        &self,
        bridge_id: &str,
        attachment: &BridgeAttachment,
    ) -> Result<(), ContentStoreError>;
    async fn read(&self, handle: &str) -> Result<Vec<u8>, ContentStoreError>;
}

/// Owner-private file content with deterministic idempotency and file-URI handles ACP can open.
pub struct FileContentStore {
    root: PathBuf,
    root_uri: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ContentMetadata {
    schema_version: u64,
    bridge_id: String,
    external_event_id: String,
    media_type: String,
    name: Option<String>,
    handle: String,
    size: u64,
    sha256: String,
}

impl FileContentStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ContentStoreError> {
        let requested = path.as_ref();
        std::fs::create_dir_all(requested).map_err(|_| ContentStoreError::Unavailable)?;
        let details =
            std::fs::symlink_metadata(requested).map_err(|_| ContentStoreError::Unavailable)?;
        if !details.is_dir() || details.file_type().is_symlink() {
            return Err(ContentStoreError::Unavailable);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(requested, std::fs::Permissions::from_mode(0o700))
                .map_err(|_| ContentStoreError::Unavailable)?;
        }
        let root = requested
            .canonicalize()
            .map_err(|_| ContentStoreError::Unavailable)?;
        let root_uri = file_uri(&root)?;
        Ok(Self { root, root_uri })
    }

    fn paths(&self, handle: &str) -> Result<(PathBuf, PathBuf), ContentStoreError> {
        let prefix = format!("{}/", self.root_uri);
        let name = handle
            .strip_prefix(&prefix)
            .ok_or(ContentStoreError::UnknownHandle)?;
        let token = name
            .strip_prefix("content_")
            .and_then(|value| value.strip_suffix(".blob"))
            .ok_or(ContentStoreError::UnknownHandle)?;
        if token.len() != 64 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(ContentStoreError::UnknownHandle);
        }
        Ok((
            self.root.join(name),
            self.root.join(format!("content_{token}.json")),
        ))
    }

    async fn metadata(&self, handle: &str) -> Result<ContentMetadata, ContentStoreError> {
        let (_, metadata_path) = self.paths(handle)?;
        let bytes = read_private(&metadata_path, 64 * 1024).await?;
        let metadata = serde_json::from_slice::<ContentMetadata>(&bytes)
            .map_err(|_| ContentStoreError::InvalidContent)?;
        if metadata.handle != handle {
            return Err(ContentStoreError::InvalidContent);
        }
        Ok(metadata)
    }

    async fn verified_bytes(
        &self,
        metadata: &ContentMetadata,
    ) -> Result<Vec<u8>, ContentStoreError> {
        let (blob, _) = self.paths(&metadata.handle)?;
        let bytes = read_private(&blob, MAX_CONTENT_BYTES).await?;
        if bytes.len() as u64 != metadata.size
            || format!("{:x}", Sha256::digest(&bytes)) != metadata.sha256
        {
            return Err(ContentStoreError::InvalidContent);
        }
        Ok(bytes)
    }
}

#[async_trait]
impl ContentStore for FileContentStore {
    async fn put(&self, request: ContentUpload) -> Result<StoredContent, ContentStoreError> {
        validate_upload(&request)?;
        let sha256 = format!("{:x}", Sha256::digest(&request.bytes));
        let token = upload_token(&request);
        let handle = format!("{}/content_{token}.blob", self.root_uri);
        let (blob, metadata_path) = self.paths(&handle)?;
        let metadata = ContentMetadata {
            schema_version: 1,
            bridge_id: request.bridge_id,
            external_event_id: request.external_event_id,
            media_type: request.media_type.clone(),
            name: request.name.clone(),
            handle: handle.clone(),
            size: request.bytes.len() as u64,
            sha256: sha256.clone(),
        };
        if metadata_path.exists() {
            let existing = self.metadata(&handle).await?;
            if existing != metadata || self.verified_bytes(&existing).await? != request.bytes {
                return Err(ContentStoreError::InvalidContent);
            }
        } else {
            if blob.exists() {
                let existing = read_private(&blob, MAX_CONTENT_BYTES).await?;
                if existing != request.bytes {
                    return Err(ContentStoreError::InvalidContent);
                }
            } else {
                write_private(&self.root, &blob, &request.bytes).await?;
            }
            let encoded =
                serde_json::to_vec(&metadata).map_err(|_| ContentStoreError::InvalidContent)?;
            write_private(&self.root, &metadata_path, &encoded).await?;
        }
        Ok(StoredContent {
            attachment: BridgeAttachment {
                media_type: request.media_type,
                name: request.name,
                content_handle: handle,
            },
            size: metadata.size,
            sha256,
        })
    }

    async fn owns(
        &self,
        bridge_id: &str,
        attachment: &BridgeAttachment,
    ) -> Result<(), ContentStoreError> {
        let metadata = self.metadata(&attachment.content_handle).await?;
        if metadata.bridge_id != bridge_id
            || metadata.media_type != attachment.media_type
            || metadata.name != attachment.name
        {
            return Err(ContentStoreError::UnknownHandle);
        }
        self.verified_bytes(&metadata).await.map(|_| ())
    }

    async fn read(&self, handle: &str) -> Result<Vec<u8>, ContentStoreError> {
        let metadata = self.metadata(handle).await?;
        self.verified_bytes(&metadata).await
    }
}

#[derive(Default)]
pub struct InMemoryContentStore {
    entries: Mutex<HashMap<String, (ContentMetadata, Vec<u8>)>>,
}

#[async_trait]
impl ContentStore for InMemoryContentStore {
    async fn put(&self, request: ContentUpload) -> Result<StoredContent, ContentStoreError> {
        validate_upload(&request)?;
        let sha256 = format!("{:x}", Sha256::digest(&request.bytes));
        let token = upload_token(&request);
        let handle = format!("file:///in-memory/content_{token}.blob");
        let metadata = ContentMetadata {
            schema_version: 1,
            bridge_id: request.bridge_id,
            external_event_id: request.external_event_id,
            media_type: request.media_type.clone(),
            name: request.name.clone(),
            handle: handle.clone(),
            size: request.bytes.len() as u64,
            sha256: sha256.clone(),
        };
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| ContentStoreError::Unavailable)?;
        if let Some(existing) = entries.get(&handle) {
            if existing != &(metadata.clone(), request.bytes.clone()) {
                return Err(ContentStoreError::InvalidContent);
            }
        } else {
            entries.insert(handle.clone(), (metadata.clone(), request.bytes));
        }
        Ok(StoredContent {
            attachment: BridgeAttachment {
                media_type: request.media_type,
                name: request.name,
                content_handle: handle,
            },
            size: metadata.size,
            sha256,
        })
    }

    async fn owns(
        &self,
        bridge_id: &str,
        attachment: &BridgeAttachment,
    ) -> Result<(), ContentStoreError> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| ContentStoreError::Unavailable)?;
        let (metadata, _) = entries
            .get(&attachment.content_handle)
            .ok_or(ContentStoreError::UnknownHandle)?;
        if metadata.bridge_id != bridge_id
            || metadata.media_type != attachment.media_type
            || metadata.name != attachment.name
        {
            return Err(ContentStoreError::UnknownHandle);
        }
        Ok(())
    }

    async fn read(&self, handle: &str) -> Result<Vec<u8>, ContentStoreError> {
        self.entries
            .lock()
            .map_err(|_| ContentStoreError::Unavailable)?
            .get(handle)
            .map(|(_, bytes)| bytes.clone())
            .ok_or(ContentStoreError::UnknownHandle)
    }
}

fn validate_upload(request: &ContentUpload) -> Result<(), ContentStoreError> {
    if request.bridge_id.trim().is_empty()
        || request.external_event_id.trim().is_empty()
        || request.media_type.trim().is_empty()
        || request.bytes.is_empty()
        || request.bytes.len() > MAX_CONTENT_BYTES
        || request.bridge_id.len() > 512
        || request.external_event_id.len() > 1024
        || request.media_type.len() > 255
        || request
            .name
            .as_ref()
            .is_some_and(|name| name.is_empty() || name.len() > 1024)
    {
        return Err(ContentStoreError::InvalidContent);
    }
    Ok(())
}

fn upload_token(request: &ContentUpload) -> String {
    let mut digest = Sha256::new();
    for value in [
        request.bridge_id.as_bytes(),
        request.external_event_id.as_bytes(),
        request.media_type.as_bytes(),
        request.name.as_deref().unwrap_or("").as_bytes(),
        &request.bytes,
    ] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    format!("{:x}", digest.finalize())
}

fn file_uri(path: &Path) -> Result<String, ContentStoreError> {
    let raw = path
        .to_str()
        .ok_or(ContentStoreError::Unavailable)?
        .as_bytes();
    let mut encoded = String::from("file://");
    for byte in raw {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(*byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    Ok(encoded)
}

async fn read_private(path: &Path, maximum: usize) -> Result<Vec<u8>, ContentStoreError> {
    let metadata = tokio::fs::symlink_metadata(path).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            ContentStoreError::UnknownHandle
        } else {
            ContentStoreError::Unavailable
        }
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(ContentStoreError::InvalidContent);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(ContentStoreError::InvalidContent);
        }
    }
    let mut bytes = Vec::new();
    tokio::fs::File::open(path)
        .await
        .map_err(|_| ContentStoreError::Unavailable)?
        .take((maximum + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| ContentStoreError::Unavailable)?;
    if bytes.len() > maximum {
        return Err(ContentStoreError::InvalidContent);
    }
    Ok(bytes)
}

async fn write_private(root: &Path, path: &Path, bytes: &[u8]) -> Result<(), ContentStoreError> {
    let temporary = root.join(format!(".content-{}.tmp", Uuid::new_v4()));
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let result = async {
        let mut file = options
            .open(&temporary)
            .await
            .map_err(|_| ContentStoreError::Unavailable)?;
        file.write_all(bytes)
            .await
            .map_err(|_| ContentStoreError::Unavailable)?;
        file.sync_all()
            .await
            .map_err(|_| ContentStoreError::Unavailable)?;
        drop(file);
        tokio::fs::rename(&temporary, path)
            .await
            .map_err(|_| ContentStoreError::Unavailable)?;
        tokio::fs::File::open(root)
            .await
            .map_err(|_| ContentStoreError::Unavailable)?
            .sync_all()
            .await
            .map_err(|_| ContentStoreError::Unavailable)
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::{ContentStore, ContentUpload, FileContentStore, MAX_CONTENT_BYTES};

    fn upload(bytes: &[u8]) -> ContentUpload {
        ContentUpload {
            bridge_id: "whatsapp".into(),
            external_event_id: "chat:message-1".into(),
            media_type: "image/jpeg".into(),
            name: Some("photo.jpg".into()),
            bytes: bytes.to_vec(),
        }
    }

    #[tokio::test]
    async fn file_content_is_private_idempotent_and_bridge_bound() {
        let directory = tempfile::tempdir().expect("temporary content directory");
        let root = directory.path().join("private content");
        let store = FileContentStore::open(&root).expect("content store opens");
        let first = store
            .put(upload(b"image bytes"))
            .await
            .expect("content stores");
        let second = store
            .put(upload(b"image bytes"))
            .await
            .expect("retry deduplicates");
        assert_eq!(first, second);
        assert!(first.attachment.content_handle.starts_with("file://"));
        assert!(
            first
                .attachment
                .content_handle
                .contains("private%20content")
        );
        assert_eq!(
            store
                .read(&first.attachment.content_handle)
                .await
                .expect("content reads"),
            b"image bytes"
        );
        store
            .owns("whatsapp", &first.attachment)
            .await
            .expect("owner validates");
        assert!(store.owns("other", &first.attachment).await.is_err());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for entry in std::fs::read_dir(&root).expect("content entries") {
                assert_eq!(
                    entry
                        .expect("content entry")
                        .metadata()
                        .expect("metadata")
                        .permissions()
                        .mode()
                        & 0o777,
                    0o600
                );
            }
        }
    }

    #[tokio::test]
    async fn empty_and_oversized_content_fail_closed() {
        let directory = tempfile::tempdir().expect("temporary content directory");
        let store = FileContentStore::open(directory.path()).expect("content store opens");
        assert!(store.put(upload(&[])).await.is_err());
        assert!(
            store
                .put(upload(&vec![0; MAX_CONTENT_BYTES + 1]))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn changed_content_and_forged_handles_are_rejected() {
        let directory = tempfile::tempdir().expect("temporary content directory");
        let store = FileContentStore::open(directory.path()).expect("content store opens");
        let stored = store
            .put(upload(b"original"))
            .await
            .expect("content stores");
        assert!(
            store
                .read("file:///tmp/content_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.blob")
                .await
                .is_err()
        );
        let (blob, _) = store
            .paths(&stored.attachment.content_handle)
            .expect("stored path resolves");
        std::fs::write(blob, b"tampered").expect("fixture tampers content");
        assert!(store.owns("whatsapp", &stored.attachment).await.is_err());
    }
}
