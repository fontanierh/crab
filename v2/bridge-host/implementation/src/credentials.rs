use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Mutex,
};

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

const MAX_CREDENTIAL_BYTES: usize = 1024 * 1024;

/// Credential-provider failures carry no secret-bearing payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialStoreError {
    Unavailable,
    UnknownHandle,
    InvalidCredential,
}

/// Opaque credential storage. Bridge state persists only returned handles.
#[async_trait]
pub trait CredentialStore: Send + Sync {
    async fn put(&self, bridge_id: &str, secret_json: &str)
    -> Result<String, CredentialStoreError>;
    async fn get(&self, handle: &str) -> Result<String, CredentialStoreError>;
    async fn invalidate(&self, handle: &str) -> Result<(), CredentialStoreError>;
}

/// Local credential provider with opaque handles and mode-0600 files.
pub struct FileCredentialStore {
    root: PathBuf,
}

impl FileCredentialStore {
    /// Create or open a private credential directory.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CredentialStoreError> {
        let root = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&root).map_err(|_| CredentialStoreError::Unavailable)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
                .map_err(|_| CredentialStoreError::Unavailable)?;
        }
        Ok(Self { root })
    }

    fn path(&self, handle: &str) -> Result<PathBuf, CredentialStoreError> {
        if !valid_handle(handle) {
            return Err(CredentialStoreError::UnknownHandle);
        }
        Ok(self.root.join(format!("{handle}.json")))
    }
}

#[async_trait]
impl CredentialStore for FileCredentialStore {
    async fn put(
        &self,
        bridge_id: &str,
        secret_json: &str,
    ) -> Result<String, CredentialStoreError> {
        validate_secret(bridge_id, secret_json)?;
        let handle = format!("credential_{}", Uuid::new_v4());
        let path = self.path(&handle)?;
        let temporary = self.root.join(format!(".{handle}.tmp"));
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let result = async {
            let mut file = options
                .open(&temporary)
                .await
                .map_err(|_| CredentialStoreError::Unavailable)?;
            file.write_all(secret_json.as_bytes())
                .await
                .map_err(|_| CredentialStoreError::Unavailable)?;
            file.sync_all()
                .await
                .map_err(|_| CredentialStoreError::Unavailable)?;
            drop(file);
            tokio::fs::rename(&temporary, &path)
                .await
                .map_err(|_| CredentialStoreError::Unavailable)?;
            sync_directory(&self.root).await
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        result?;
        Ok(handle)
    }

    async fn get(&self, handle: &str) -> Result<String, CredentialStoreError> {
        let file = tokio::fs::File::open(self.path(handle)?)
            .await
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    CredentialStoreError::UnknownHandle
                } else {
                    CredentialStoreError::Unavailable
                }
            })?;
        let mut bytes = Vec::new();
        file.take((MAX_CREDENTIAL_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| CredentialStoreError::Unavailable)?;
        if bytes.len() > MAX_CREDENTIAL_BYTES {
            return Err(CredentialStoreError::InvalidCredential);
        }
        let secret =
            String::from_utf8(bytes).map_err(|_| CredentialStoreError::InvalidCredential)?;
        serde_json::from_str::<serde_json::Value>(&secret)
            .map_err(|_| CredentialStoreError::InvalidCredential)?;
        Ok(secret)
    }

    async fn invalidate(&self, handle: &str) -> Result<(), CredentialStoreError> {
        match tokio::fs::remove_file(self.path(handle)?).await {
            Ok(()) => sync_directory(&self.root).await,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(CredentialStoreError::Unavailable),
        }
    }
}

/// Ephemeral provider for tests and in-memory compositions.
#[derive(Default)]
pub struct InMemoryCredentialStore {
    secrets: Mutex<HashMap<String, String>>,
}

#[async_trait]
impl CredentialStore for InMemoryCredentialStore {
    async fn put(
        &self,
        bridge_id: &str,
        secret_json: &str,
    ) -> Result<String, CredentialStoreError> {
        validate_secret(bridge_id, secret_json)?;
        let handle = format!("credential_{}", Uuid::new_v4());
        self.secrets
            .lock()
            .map_err(|_| CredentialStoreError::Unavailable)?
            .insert(handle.clone(), secret_json.to_owned());
        Ok(handle)
    }

    async fn get(&self, handle: &str) -> Result<String, CredentialStoreError> {
        self.secrets
            .lock()
            .map_err(|_| CredentialStoreError::Unavailable)?
            .get(handle)
            .cloned()
            .ok_or(CredentialStoreError::UnknownHandle)
    }

    async fn invalidate(&self, handle: &str) -> Result<(), CredentialStoreError> {
        self.secrets
            .lock()
            .map_err(|_| CredentialStoreError::Unavailable)?
            .remove(handle);
        Ok(())
    }
}

async fn sync_directory(path: &Path) -> Result<(), CredentialStoreError> {
    let directory = tokio::fs::File::open(path)
        .await
        .map_err(|_| CredentialStoreError::Unavailable)?;
    directory
        .sync_all()
        .await
        .map_err(|_| CredentialStoreError::Unavailable)
}

fn validate_secret(bridge_id: &str, secret_json: &str) -> Result<(), CredentialStoreError> {
    if bridge_id.trim().is_empty() || secret_json.len() > MAX_CREDENTIAL_BYTES {
        return Err(CredentialStoreError::InvalidCredential);
    }
    serde_json::from_str::<serde_json::Value>(secret_json)
        .map_err(|_| CredentialStoreError::InvalidCredential)?;
    Ok(())
}

fn valid_handle(handle: &str) -> bool {
    handle.starts_with("credential_")
        && handle.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '_' || character == '-'
        })
}

#[cfg(test)]
mod tests {
    use super::{CredentialStore, FileCredentialStore};

    #[tokio::test]
    async fn file_store_round_trips_and_invalidates_opaque_handles() {
        let directory = tempfile::tempdir().expect("temporary credential directory");
        let store = FileCredentialStore::open(directory.path()).expect("store opens");
        let handle = store
            .put("bridge-1", r#"{"token":"secret"}"#)
            .await
            .expect("secret persists");
        assert!(!handle.contains("secret"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(
                std::fs::metadata(&store.root)
                    .expect("credential directory metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
            assert_eq!(
                std::fs::metadata(store.path(&handle).expect("credential path"))
                    .expect("credential metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        assert_eq!(
            store.get(&handle).await.expect("secret loads"),
            r#"{"token":"secret"}"#
        );
        store.invalidate(&handle).await.expect("secret invalidates");
        assert!(store.get(&handle).await.is_err());
    }
}
