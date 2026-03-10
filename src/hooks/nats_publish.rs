//! NATS JetStream publish hook for event-sourced chat integration.
//!
//! Publishes inbound/outbound message events to the `otoflo_chat` JetStream
//! stream so the downstream GraphProjector and ChatRelayConsumer process them
//! automatically.
//!
//! Subject pattern:
//! - `otoflo.chat.{room}.user`  — inbound user messages
//! - `otoflo.chat.{room}.agent` — outbound agent responses
//!
//! This hook is **fail-open**: NATS unavailability never blocks the agent.

use std::sync::Arc;
use std::time::Duration;

use async_nats::Client as NatsClient;
use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::RwLock;

use super::hook::{Hook, HookContext, HookError, HookEvent, HookFailureMode, HookOutcome, HookPoint};

/// Configuration for the NATS publish hook.
#[derive(Debug, Clone)]
pub struct NatsPublishConfig {
    /// NATS server URL (default: `nats://localhost:4222`).
    pub nats_url: String,
    /// Room name for subject routing (default: `general`).
    pub room: String,
    /// Agent name included in published events.
    pub agent_name: String,
}

impl Default for NatsPublishConfig {
    fn default() -> Self {
        Self {
            nats_url: std::env::var("NATS_URL")
                .unwrap_or_else(|_| "nats://localhost:4222".to_string()),
            room: std::env::var("NATS_ROOM")
                .unwrap_or_else(|_| "general".to_string()),
            agent_name: std::env::var("AGENT_NAME")
                .unwrap_or_else(|_| "ironclaw".to_string()),
        }
    }
}

/// JSON payload published to NATS, matching what `ChatRelayConsumer` expects.
#[derive(Debug, Serialize)]
struct ChatEvent {
    message_id: String,
    room: String,
    role: String,
    content: String,
    user_id: String,
    channel: String,
    timestamp: String,
    agent_name: String,
}

/// Hook that publishes chat events to NATS JetStream.
///
/// Fires on `BeforeInbound` (user messages) and `BeforeOutbound` (agent
/// responses). Uses fire-and-forget via `tokio::spawn` so NATS latency
/// never blocks the agent loop.
pub struct NatsPublishHook {
    config: NatsPublishConfig,
    client: Arc<RwLock<Option<NatsClient>>>,
}

impl NatsPublishHook {
    /// Create a new NATS publish hook. Connects lazily on first event.
    pub fn new(config: NatsPublishConfig) -> Self {
        Self {
            config,
            client: Arc::new(RwLock::new(None)),
        }
    }

    /// Create with default config from environment variables.
    pub fn from_env() -> Self {
        Self::new(NatsPublishConfig::default())
    }

    /// Ensure we have a NATS connection, creating one if needed.
    async fn ensure_connected(&self) -> Option<NatsClient> {
        // Fast path: already connected
        {
            let guard = self.client.read().await;
            if let Some(ref client) = *guard {
                if client.connection_state() == async_nats::connection::State::Connected {
                    return Some(client.clone());
                }
            }
        }

        // Slow path: (re)connect
        let mut guard = self.client.write().await;

        // Double-check after acquiring write lock
        if let Some(ref client) = *guard {
            if client.connection_state() == async_nats::connection::State::Connected {
                return Some(client.clone());
            }
        }

        match async_nats::ConnectOptions::new()
            .name("ironclaw-nats-hook")
            .retry_on_initial_connect()
            .connect(&self.config.nats_url)
            .await
        {
            Ok(client) => {
                tracing::info!(
                    url = %self.config.nats_url,
                    "NATS publish hook connected"
                );
                let cloned = client.clone();
                *guard = Some(client);
                Some(cloned)
            }
            Err(e) => {
                tracing::warn!(
                    url = %self.config.nats_url,
                    error = %e,
                    "NATS publish hook connection failed (will retry on next event)"
                );
                None
            }
        }
    }

    /// Publish a chat event to NATS (fire-and-forget).
    fn spawn_publish(&self, event: ChatEvent) {
        let client = self.client.clone();
        let nats_url = self.config.nats_url.clone();
        let subject = format!("otoflo.chat.{}.{}", event.room, event.role);

        tokio::spawn(async move {
            // Get or establish connection
            let nats_client = {
                let guard = client.read().await;
                match guard.as_ref() {
                    Some(c) if c.connection_state() == async_nats::connection::State::Connected => {
                        Some(c.clone())
                    }
                    _ => None,
                }
            };

            let nats_client = match nats_client {
                Some(c) => c,
                None => {
                    tracing::debug!("NATS not connected, dropping event for {}", subject);
                    return;
                }
            };

            let payload = match serde_json::to_vec(&event) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to serialize NATS chat event");
                    return;
                }
            };

            if let Err(e) = nats_client.publish(subject.clone(), payload.into()).await {
                tracing::warn!(
                    subject = %subject,
                    error = %e,
                    url = %nats_url,
                    "NATS publish failed"
                );
            } else {
                tracing::debug!(subject = %subject, "Published chat event to NATS");
            }
        });
    }
}

#[async_trait]
impl Hook for NatsPublishHook {
    fn name(&self) -> &str {
        "otoflo.nats_publish"
    }

    fn hook_points(&self) -> &[HookPoint] {
        &[HookPoint::BeforeInbound, HookPoint::BeforeOutbound]
    }

    fn failure_mode(&self) -> HookFailureMode {
        HookFailureMode::FailOpen
    }

    fn timeout(&self) -> Duration {
        // Generous timeout since we fire-and-forget via spawn anyway.
        // This only applies if the registry enforces a timeout wrapper.
        Duration::from_secs(2)
    }

    async fn execute(
        &self,
        event: &HookEvent,
        _ctx: &HookContext,
    ) -> Result<HookOutcome, HookError> {
        // Ensure NATS is connected (lazy init)
        self.ensure_connected().await;

        let chat_event = match event {
            HookEvent::Inbound {
                user_id,
                channel,
                content,
                thread_id: _,
            } => ChatEvent {
                message_id: uuid::Uuid::new_v4().to_string(),
                room: self.config.room.clone(),
                role: "user".to_string(),
                content: content.clone(),
                user_id: user_id.clone(),
                channel: channel.clone(),
                timestamp: chrono::Utc::now().to_rfc3339(),
                agent_name: self.config.agent_name.clone(),
            },
            HookEvent::Outbound {
                user_id,
                channel,
                content,
                thread_id: _,
            } => ChatEvent {
                message_id: uuid::Uuid::new_v4().to_string(),
                room: self.config.room.clone(),
                role: "agent".to_string(),
                content: content.clone(),
                user_id: user_id.clone(),
                channel: channel.clone(),
                timestamp: chrono::Utc::now().to_rfc3339(),
                agent_name: self.config.agent_name.clone(),
            },
            // Other hook points — no-op
            _ => return Ok(HookOutcome::ok()),
        };

        self.spawn_publish(chat_event);

        // Never modify or reject — this is a passive observer hook
        Ok(HookOutcome::ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_from_env() {
        let config = NatsPublishConfig::default();
        assert!(config.nats_url.starts_with("nats://"));
        assert!(!config.room.is_empty());
        assert!(!config.agent_name.is_empty());
    }

    #[test]
    fn test_chat_event_serialization() {
        let event = ChatEvent {
            message_id: "test-id".to_string(),
            room: "general".to_string(),
            role: "user".to_string(),
            content: "hello".to_string(),
            user_id: "user-1".to_string(),
            channel: "http".to_string(),
            timestamp: "2026-03-10T03:00:00Z".to_string(),
            agent_name: "rook".to_string(),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"role\":\"user\""));
        assert!(json.contains("\"room\":\"general\""));
        assert!(json.contains("\"content\":\"hello\""));
    }

    #[tokio::test]
    async fn test_hook_metadata() {
        let hook = NatsPublishHook::from_env();
        assert_eq!(hook.name(), "otoflo.nats_publish");
        assert_eq!(hook.hook_points(), &[HookPoint::BeforeInbound, HookPoint::BeforeOutbound]);
        assert_eq!(hook.failure_mode(), HookFailureMode::FailOpen);
    }

    #[tokio::test]
    async fn test_execute_without_nats_does_not_panic() {
        // Verifies the hook works gracefully when NATS is unavailable
        let hook = NatsPublishHook::new(NatsPublishConfig {
            nats_url: "nats://127.0.0.1:1".to_string(), // unreachable
            room: "test".to_string(),
            agent_name: "test-agent".to_string(),
        });

        let event = HookEvent::Inbound {
            user_id: "user-1".to_string(),
            channel: "http".to_string(),
            content: "hello".to_string(),
            thread_id: None,
        };

        let result = hook.execute(&event, &HookContext::default()).await;
        assert!(result.is_ok());
        match result.unwrap() {
            HookOutcome::Continue { modified } => assert!(modified.is_none()),
            _ => panic!("Expected Continue"),
        }
    }
}
