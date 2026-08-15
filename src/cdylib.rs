//! cdylib sync bridge — adapts the async [`CommandBackendPlugin`]
//! ([`mcpg_plugin_protocol::BackendPlugin`]) onto the sync FFI trait the
//! cdylib vtable expects ([`SyncBackendPlugin`]).
//!
//! Minimal, like the grpc/graphql bridges: command is request/reply, so
//! it inherits the buffered `execute_streaming` default and the no-op
//! `cancel_stream` / `complete_template_variable`. Only manifest / kind
//! / register_profile / execute / audit_metadata are forwarded, each
//! `block_on`-ing the async inner plugin on a private multi-thread
//! runtime. The make-time [`HostHandle`] is wrapped as an
//! `Arc<dyn BackendHost>` — command never calls the host (no `cred://`),
//! but `register_profile`'s signature requires one.

use std::sync::Arc;

use mcpg_plugin_protocol::{
    BackendError, BackendPlugin, BackendRequest, BackendResponse, PluginManifest,
};
use mcpg_plugin_sdk::ffi::SyncBackendPlugin;
use mcpg_plugin_sdk::{HostHandle, HostHandleBackendHost};

use crate::CommandBackendPlugin;

fn build_bridge_runtime(thread_name: &str) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name(thread_name.to_owned())
        .enable_all()
        .build()
        .unwrap_or_else(|e| panic!("command cdylib: tokio runtime init failed: {e}"))
}

/// `SyncBackendPlugin` bridge over [`CommandBackendPlugin`].
pub struct CommandBackendCdylib {
    inner: CommandBackendPlugin,
    host: Arc<dyn mcpg_plugin_protocol::BackendHost>,
    rt: tokio::runtime::Runtime,
}

impl CommandBackendCdylib {
    /// Infallible cdylib factory. `config_json` is ignored — command
    /// carries no plugin-level config (per-binding command/args/limits
    /// arrive via `register_profile`).
    pub fn from_host_config(_config_json: &str, host: HostHandle) -> Self {
        Self {
            inner: CommandBackendPlugin::new(),
            host: Arc::new(HostHandleBackendHost::new(host)),
            rt: build_bridge_runtime("mcpg-backend-command"),
        }
    }
}

impl SyncBackendPlugin for CommandBackendCdylib {
    fn manifest(&self) -> &PluginManifest {
        BackendPlugin::manifest(&self.inner)
    }

    fn kind(&self) -> &str {
        BackendPlugin::kind(&self.inner)
    }

    fn register_profile(
        &self,
        profile_name: &str,
        spec: &serde_json::Value,
    ) -> Result<(), BackendError> {
        self.rt.block_on(BackendPlugin::register_profile(
            &self.inner,
            profile_name,
            spec,
            Arc::clone(&self.host),
        ))
    }

    fn execute(
        &self,
        profile_name: &str,
        request: BackendRequest,
    ) -> Result<BackendResponse, BackendError> {
        self.rt
            .block_on(BackendPlugin::execute(&self.inner, profile_name, request))
    }

    fn audit_metadata(&self, profile_name: &str) -> serde_json::Map<String, serde_json::Value> {
        BackendPlugin::audit_metadata(&self.inner, profile_name)
    }
}

// cdylib export — one `backend` entity under `dev.mcpg.backend.command`.
mcpg_plugin_sdk::declare_plugin! {
    plugin_id: "dev.mcpg.backend.command",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[],
    backend_profile: ::mcpg_plugin_protocol::manifest::BackendProfile {
        pipeline_capable: true,
        ..::core::default::Default::default()
    },
    entities: [
        backend as binding {
            inner_name: "",
            plugin_type: CommandBackendCdylib,
            factory: |cfg, host: ::mcpg_plugin_sdk::HostHandle|
                CommandBackendCdylib::from_host_config(cfg, host),
        },
    ],
}
