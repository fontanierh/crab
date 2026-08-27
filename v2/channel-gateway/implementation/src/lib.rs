mod contract;

pub use contract::*;

use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex as StdMutex, PoisonError, Weak},
};

use boxology_contract::{CallContext, ErasedCallError};
use boxology_import_agent_host::{OpenSessionRequest, ResumeSessionRequest, SessionReference};
use boxology_import_native_channel::{
    BindChannelRequest, BindingReference, LocateBindingRequest, RecoverSessionRequest,
    ReplaceSessionRequest,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, OwnedMutexGuard};

use generated::{AgentHostImport, NativeChannelImport};

const MAX_ATTACHMENT_IDENTIFIER_BYTES: usize = 256;
const MAX_WORKING_DIRECTORY_BYTES: usize = 4 * 1024;
const MAX_SESSION_METADATA_BYTES: usize = 64 * 1024;
const MAX_NATIVE_CHANNEL_METADATA_BYTES: usize = 64 * 1024;
const MAX_BOOTSTRAP_PROMPT_BYTES: usize = 2 * 1024 * 1024;

/// Stateless lifecycle owner waiting for composition-selected imports.
pub struct ChannelGateway {
    agent_host: AgentHostImport,
    native_channel: NativeChannelImport,
    operations: AttachmentLocks,
}

impl ChannelGateway {
    /// Attach the graph-selected agent host and native-channel implementation.
    #[must_use]
    pub fn connect(agent_host: AgentHostImport, native_channel: NativeChannelImport) -> Self {
        Self {
            agent_host,
            native_channel,
            operations: AttachmentLocks::default(),
        }
    }

    async fn open_session(
        &self,
        context: CallContext,
        request: &AttachChannelRequest,
    ) -> Result<String, ChannelGatewayError> {
        self.agent_host
            .open_session(
                context,
                OpenSessionRequest {
                    agent_id: request.agent_id.clone(),
                    working_directory: request.working_directory.clone(),
                    bootstrap_prompt: request.bootstrap_prompt.clone(),
                    metadata_json: request.session_metadata_json.clone(),
                },
            )
            .await
            .map(|session| session.session_id)
            .map_err(map_agent_error)
    }

    async fn close_session(&self, context: CallContext, session_id: &str) {
        let _ = self
            .agent_host
            .close_session(
                context,
                SessionReference {
                    session_id: session_id.to_owned(),
                },
            )
            .await;
    }
}

#[boxology::implementation]
impl ChannelGateway {
    pub async fn attach_channel(
        &self,
        context: CallContext,
        request: AttachChannelRequest,
    ) -> Result<ChannelAttachment, ChannelGatewayError> {
        let desired_channel_json = attachment_envelope(&request)?;
        let _operation = self
            .operations
            .lock(&request.adapter_id, &request.channel_id)
            .await;
        let existing = match self
            .native_channel
            .find_binding(
                context.clone(),
                LocateBindingRequest {
                    channel_id: request.channel_id.clone(),
                    adapter_id: request.adapter_id.clone(),
                },
            )
            .await
        {
            Ok(binding) => Some(binding),
            Err(error) if has_domain_tag(&error, "UnknownBinding") => None,
            Err(error) => return Err(map_channel_error(error)),
        };

        if let Some(binding) = existing {
            let live = match self
                .native_channel
                .channel_status(
                    context.clone(),
                    BindingReference {
                        binding_id: binding.binding_id.clone(),
                    },
                )
                .await
            {
                Ok(_) => true,
                Err(error) if has_domain_tag(&error, "SessionUnavailable") => false,
                Err(error) => return Err(map_channel_error(error)),
            };
            if live {
                if binding.native_channel_json != desired_channel_json {
                    return Err(ChannelGatewayError::AttachmentConflict);
                }
                return Ok(attachment(
                    binding,
                    ChannelAttachmentDisposition::ReusedLiveSession,
                ));
            }

            if binding.native_channel_json == desired_channel_json {
                match self
                    .agent_host
                    .resume_session(
                        context.clone(),
                        ResumeSessionRequest {
                            session_id: binding.session_id.clone(),
                        },
                    )
                    .await
                {
                    Ok(session) if session.session_id == binding.session_id => {
                        let recovered = self
                            .native_channel
                            .recover_session(
                                context.clone(),
                                RecoverSessionRequest {
                                    binding_id: binding.binding_id,
                                    expected_session_id: binding.session_id,
                                },
                            )
                            .await
                            .map_err(map_channel_error)?;
                        return Ok(attachment(
                            recovered,
                            ChannelAttachmentDisposition::ResumedUnavailableSession,
                        ));
                    }
                    Ok(_) => return Err(ChannelGatewayError::AgentUnavailable),
                    Err(error)
                        if has_domain_tag(&error, "SessionResumeUnavailable")
                            || has_domain_tag(&error, "UnknownSession") => {}
                    Err(error) => return Err(map_agent_error(error)),
                }
            }

            let session_id = self.open_session(context.clone(), &request).await?;
            let replaced = self
                .native_channel
                .replace_session(
                    context.clone(),
                    ReplaceSessionRequest {
                        binding_id: binding.binding_id,
                        expected_session_id: binding.session_id,
                        fresh_session_id: session_id.clone(),
                        fresh_native_channel_json: Some(desired_channel_json),
                        reason: "channel gateway recovered unavailable session".into(),
                    },
                )
                .await;
            return match replaced {
                Ok(binding) => Ok(attachment(
                    binding,
                    ChannelAttachmentDisposition::ReplacedUnavailableSession,
                )),
                Err(error) => {
                    self.close_session(context, &session_id).await;
                    Err(map_channel_error(error))
                }
            };
        }

        let session_id = self.open_session(context.clone(), &request).await?;
        let bound = self
            .native_channel
            .bind_channel(
                context.clone(),
                BindChannelRequest {
                    channel_id: request.channel_id,
                    adapter_id: request.adapter_id,
                    session_id: session_id.clone(),
                    native_channel_json: desired_channel_json,
                },
            )
            .await;
        match bound {
            Ok(binding) => Ok(attachment(binding, ChannelAttachmentDisposition::Created)),
            Err(error) => {
                self.close_session(context, &session_id).await;
                Err(map_channel_error(error))
            }
        }
    }
}

#[derive(Default)]
struct AttachmentLocks {
    locks: StdMutex<HashMap<AttachmentIdentity, WeakAttachmentLock>>,
}

type AttachmentIdentity = (String, String);
type WeakAttachmentLock = Weak<Mutex<()>>;

impl AttachmentLocks {
    async fn lock(&self, adapter_id: &str, channel_id: &str) -> OwnedMutexGuard<()> {
        let key = (adapter_id.to_owned(), channel_id.to_owned());
        let lock = {
            let mut locks = self.locks.lock().unwrap_or_else(PoisonError::into_inner);
            locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(Mutex::new(()));
                locks.insert(key, Arc::downgrade(&lock));
                lock
            }
        };
        lock.lock_owned().await
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

fn attachment_envelope(request: &AttachChannelRequest) -> Result<String, ChannelGatewayError> {
    if !valid_identifier(&request.channel_id)
        || !valid_identifier(&request.adapter_id)
        || !valid_identifier(&request.agent_id)
        || request.working_directory.trim().is_empty()
        || request.working_directory.len() > MAX_WORKING_DIRECTORY_BYTES
        || !Path::new(&request.working_directory).is_absolute()
        || request
            .bootstrap_prompt
            .as_ref()
            .is_some_and(|prompt| prompt.len() > MAX_BOOTSTRAP_PROMPT_BYTES)
        || request.session_metadata_json.len() > MAX_SESSION_METADATA_BYTES
        || request.native_channel_json.len() > MAX_NATIVE_CHANNEL_METADATA_BYTES
    {
        return Err(ChannelGatewayError::InvalidRequest);
    }
    let session_metadata: Value = serde_json::from_str(&request.session_metadata_json)
        .map_err(|_| ChannelGatewayError::InvalidRequest)?;
    let native_channel: Value = serde_json::from_str(&request.native_channel_json)
        .map_err(|_| ChannelGatewayError::InvalidRequest)?;
    if !session_metadata.is_object() || !native_channel.is_object() {
        return Err(ChannelGatewayError::InvalidRequest);
    }
    let fingerprint_source = serde_json::to_vec(&json!({
        "agentId": request.agent_id,
        "workingDirectory": request.working_directory,
        "bootstrapPrompt": request.bootstrap_prompt,
        "sessionMetadata": session_metadata,
        "nativeChannel": native_channel,
    }))
    .map_err(|_| ChannelGatewayError::InvalidRequest)?;
    let fingerprint = format!("{:x}", Sha256::digest(fingerprint_source));
    serde_json::to_string(&json!({
        "adapter": native_channel,
        "crabAttachment": {"schema": 1, "fingerprint": fingerprint},
    }))
    .map_err(|_| ChannelGatewayError::InvalidRequest)
}

fn valid_identifier(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= MAX_ATTACHMENT_IDENTIFIER_BYTES
}

fn attachment(
    binding: boxology_import_native_channel::ChannelBinding,
    disposition: ChannelAttachmentDisposition,
) -> ChannelAttachment {
    ChannelAttachment {
        binding_id: binding.binding_id,
        channel_id: binding.channel_id,
        adapter_id: binding.adapter_id,
        session_id: binding.session_id,
        disposition,
    }
}

fn has_domain_tag(error: &ErasedCallError, expected: &str) -> bool {
    matches!(error, ErasedCallError::Domain { error_tag, .. } if error_tag == expected)
}

fn map_agent_error(error: ErasedCallError) -> ChannelGatewayError {
    match error {
        ErasedCallError::Domain { error_tag, .. } if error_tag == "UnknownAgent" => {
            ChannelGatewayError::UnknownAgent
        }
        ErasedCallError::Domain { error_tag, .. } if error_tag == "StorageUnavailable" => {
            ChannelGatewayError::StorageUnavailable
        }
        _ => ChannelGatewayError::AgentUnavailable,
    }
}

fn map_channel_error(error: ErasedCallError) -> ChannelGatewayError {
    match error {
        ErasedCallError::Domain { error_tag, .. } if error_tag == "StorageUnavailable" => {
            ChannelGatewayError::StorageUnavailable
        }
        ErasedCallError::Domain { error_tag, .. }
            if matches!(error_tag.as_str(), "AlreadyBound" | "SessionMismatch") =>
        {
            ChannelGatewayError::AttachmentConflict
        }
        _ => ChannelGatewayError::ChannelUnavailable,
    }
}

pub mod generated {
    include!("../../generated/adapter/adapter.rs");
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use boxology_contract::CapabilityId;
    use tokio::time::timeout;

    use super::{
        AttachChannelRequest, AttachmentLocks, ChannelGatewayError,
        MAX_ATTACHMENT_IDENTIFIER_BYTES, MAX_BOOTSTRAP_PROMPT_BYTES,
        MAX_NATIVE_CHANNEL_METADATA_BYTES, MAX_SESSION_METADATA_BYTES, MAX_WORKING_DIRECTORY_BYTES,
        attachment_envelope, generated,
    };

    #[test]
    fn contract_exposes_only_idempotent_attachment() {
        let names = generated::implementation_descriptor()
            .contract()
            .capabilities()
            .iter()
            .map(|capability| capability.id().clone())
            .map(|capability: CapabilityId| capability.name().as_str().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(names, ["attach_channel"]);
    }

    #[tokio::test]
    async fn attachment_locks_serialize_only_matching_identities_and_prune() {
        let locks = AttachmentLocks::default();
        let first = locks.lock("t3code", "channel-a").await;

        let other_channel = timeout(
            Duration::from_millis(100),
            locks.lock("t3code", "channel-b"),
        )
        .await
        .expect("unrelated channel does not wait");
        let other_adapter = timeout(
            Duration::from_millis(100),
            locks.lock("another-ui", "channel-a"),
        )
        .await
        .expect("unrelated adapter does not wait");
        assert!(
            timeout(Duration::from_millis(20), locks.lock("t3code", "channel-a"))
                .await
                .is_err()
        );

        drop(first);
        let matching = timeout(
            Duration::from_millis(100),
            locks.lock("t3code", "channel-a"),
        )
        .await
        .expect("matching identity proceeds after release");
        drop(matching);
        drop(other_channel);
        drop(other_adapter);

        let fresh = locks.lock("t3code", "fresh").await;
        assert_eq!(locks.len(), 1);
        drop(fresh);
    }

    #[test]
    fn attachment_admission_accepts_exact_field_limits() {
        let request = AttachChannelRequest {
            channel_id: "c".repeat(MAX_ATTACHMENT_IDENTIFIER_BYTES),
            adapter_id: "a".repeat(MAX_ATTACHMENT_IDENTIFIER_BYTES),
            agent_id: "g".repeat(MAX_ATTACHMENT_IDENTIFIER_BYTES),
            working_directory: format!("/{}", "w".repeat(MAX_WORKING_DIRECTORY_BYTES - 1)),
            bootstrap_prompt: Some("p".repeat(MAX_BOOTSTRAP_PROMPT_BYTES)),
            session_metadata_json: object_json_with_len(MAX_SESSION_METADATA_BYTES),
            native_channel_json: object_json_with_len(MAX_NATIVE_CHANNEL_METADATA_BYTES),
        };

        attachment_envelope(&request).expect("exact byte limits are admitted");
    }

    #[test]
    fn attachment_admission_rejects_each_oversized_field() {
        let request = valid_request();
        let cases = [
            AttachChannelRequest {
                channel_id: "c".repeat(MAX_ATTACHMENT_IDENTIFIER_BYTES + 1),
                ..request.clone()
            },
            AttachChannelRequest {
                adapter_id: "a".repeat(MAX_ATTACHMENT_IDENTIFIER_BYTES + 1),
                ..request.clone()
            },
            AttachChannelRequest {
                agent_id: "g".repeat(MAX_ATTACHMENT_IDENTIFIER_BYTES + 1),
                ..request.clone()
            },
            AttachChannelRequest {
                working_directory: format!("/{}", "w".repeat(MAX_WORKING_DIRECTORY_BYTES)),
                ..request.clone()
            },
            AttachChannelRequest {
                bootstrap_prompt: Some("p".repeat(MAX_BOOTSTRAP_PROMPT_BYTES + 1)),
                ..request.clone()
            },
            AttachChannelRequest {
                session_metadata_json: object_json_with_len(MAX_SESSION_METADATA_BYTES + 1),
                ..request.clone()
            },
            AttachChannelRequest {
                native_channel_json: object_json_with_len(MAX_NATIVE_CHANNEL_METADATA_BYTES + 1),
                ..request
            },
        ];

        for oversized in cases {
            assert_eq!(
                attachment_envelope(&oversized),
                Err(ChannelGatewayError::InvalidRequest)
            );
        }
    }

    fn valid_request() -> AttachChannelRequest {
        AttachChannelRequest {
            channel_id: "channel".into(),
            adapter_id: "adapter".into(),
            agent_id: "agent".into(),
            working_directory: "/workspace".into(),
            bootstrap_prompt: None,
            session_metadata_json: "{}".into(),
            native_channel_json: "{}".into(),
        }
    }

    fn object_json_with_len(len: usize) -> String {
        let mut value = String::from(r#"{"value":""#);
        value.extend(std::iter::repeat_n('x', len - value.len() - 2));
        value.push_str(r#""}"#);
        assert_eq!(value.len(), len);
        value
    }
}
