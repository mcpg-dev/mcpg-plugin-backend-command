//! Command (subprocess) backend binding plugin for mcpg
//! (`kind: "command"`).
//!
//! Dispatches a tool call by spawning a local process: the call
//! arguments are serialized as a JSON document and written to the
//! child's stdin, and stdout/stderr are captured under byte +
//! wall-clock limits. The configured args support CEL templating
//! against `$arguments` / `$context` (resolved per call); the command
//! path itself is never templated. Unlike the network backends, this
//! crate pulls no HTTP/reqwest machinery — it only needs CEL
//! (`mcpg-expr`) for arg resolution.
//!
//! The structured envelope carries the process outcome (exit code,
//! stdout/stderr, truncation flags, parsed JSON) and the shared
//! `downstreamError` slot the gateway reads for `is_error` — set on a
//! timeout, non-zero exit, output-capture failure, or (when
//! `require_json_stdout`) non-JSON stdout.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mcpg_expr::{DynamicValue, ExprContext, ExprRequestContext};
use mcpg_plugin_protocol::{
    BackendError, BackendHost, BackendPlugin, BackendRequest, BackendResponse, PluginManifest,
    firstparty_manifest,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::RwLock;

pub mod cdylib;

/// Embedded plugin descriptor — passed to the gateway registrar at
/// startup.
pub const BINDING_DESCRIPTOR_YAML: &str = include_str!("../plugin.yaml");

fn default_timeout_ms() -> u64 {
    5_000
}
fn default_max_output_bytes() -> usize {
    65_536
}

/// Operator-facing spec the gateway serializes when calling
/// `register_profile`. Mirrors `CommandBackendConfig` in the gateway crate.
#[derive(Debug, Clone, Deserialize)]
struct CommandBackendSpec {
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default = "default_max_output_bytes")]
    max_output_bytes: usize,
    #[serde(default)]
    require_json_stdout: bool,
}

/// Per-binding runtime state. `command` is structural (never templated);
/// each arg is a compiled CEL template resolved per call against the
/// call's `$arguments` / `$context`.
#[derive(Clone)]
struct CommandProfile {
    command: String,
    arg_templates: Arc<Vec<DynamicValue<String>>>,
    timeout_ms: u64,
    max_output_bytes: usize,
    require_json_stdout: bool,
}

/// `BackendPlugin` implementation for `kind: "command"`.
pub struct CommandBackendPlugin {
    manifest: PluginManifest,
    profiles: RwLock<BTreeMap<String, CommandProfile>>,
}

impl Default for CommandBackendPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandBackendPlugin {
    #[must_use]
    pub fn new() -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.backend.command",
                name: "Command Binding",
                class: Backend,
            },
            profiles: RwLock::new(BTreeMap::new()),
        }
    }
}

impl std::fmt::Debug for CommandBackendPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandBackendPlugin")
            .field("id", &self.manifest.id)
            .finish()
    }
}

#[async_trait]
impl BackendPlugin for CommandBackendPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        "command"
    }

    async fn register_profile(
        &self,
        backend_name: &str,
        spec: &serde_json::Value,
        _host: Arc<dyn BackendHost>,
    ) -> Result<(), BackendError> {
        let parsed: CommandBackendSpec =
            serde_json::from_value(spec.clone()).map_err(|e| BackendError::InvalidSpec {
                message: format!("command binding spec: {e}"),
            })?;

        if parsed.command.trim().is_empty() {
            return Err(BackendError::InvalidSpec {
                message: "command must not be empty".into(),
            });
        }
        if parsed.timeout_ms == 0 {
            return Err(BackendError::InvalidSpec {
                message: "timeout_ms must be greater than 0".into(),
            });
        }
        if parsed.max_output_bytes == 0 {
            return Err(BackendError::InvalidSpec {
                message: "max_output_bytes must be greater than 0".into(),
            });
        }

        // Compile arg CEL templates at register time. Plain literals
        // (no `${`) compile to `DynamicValue::Literal` and skip the CEL
        // engine entirely.
        let mut arg_templates = Vec::with_capacity(parsed.args.len());
        for (i, arg) in parsed.args.iter().enumerate() {
            let dv = DynamicValue::<String>::parse(arg).map_err(|e| BackendError::InvalidSpec {
                message: format!("command arg[{i}] expression: {e}"),
            })?;
            arg_templates.push(dv);
        }

        self.profiles.write().await.insert(
            backend_name.to_owned(),
            CommandProfile {
                command: parsed.command,
                arg_templates: Arc::new(arg_templates),
                timeout_ms: parsed.timeout_ms,
                max_output_bytes: parsed.max_output_bytes,
                require_json_stdout: parsed.require_json_stdout,
            },
        );
        Ok(())
    }

    async fn execute(
        &self,
        backend_name: &str,
        request: BackendRequest,
    ) -> Result<BackendResponse, BackendError> {
        let profile = {
            let guard = self.profiles.read().await;
            guard
                .get(backend_name)
                .cloned()
                .ok_or_else(|| BackendError::ProfileNotFound {
                    backend_name: backend_name.to_owned(),
                })?
        };

        let arguments: Value = if request.payload.is_empty() {
            serde_json::json!({})
        } else {
            serde_json::from_slice(&request.payload).map_err(|e| BackendError::InvalidSpec {
                message: format!("command plugin payload is not valid JSON: {e}"),
            })?
        };

        let tool_name = request
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("mcpg-tool-name"))
            .map(|(_, v)| v.as_str())
            .unwrap_or(backend_name)
            .to_owned();

        // Resolve the arg templates against this call's args + identity.
        let expr_ctx = build_expr_context(&arguments, &tool_name, &request);
        let mut resolved_args = Vec::with_capacity(profile.arg_templates.len());
        for (i, tmpl) in profile.arg_templates.iter().enumerate() {
            match tmpl.resolve(&expr_ctx) {
                Ok(v) => resolved_args.push(v),
                Err(e) => {
                    let envelope = build_command_envelope(
                        &tool_name,
                        backend_name,
                        &profile,
                        &arguments,
                        None,
                        None,
                        None,
                        &[],
                        Some(&format!("evaluating command arg[{i}] expression: {e}")),
                    );
                    return Ok(envelope_response(envelope));
                }
            }
        }

        // W3C trace propagation into the child env.
        let traceparent = request
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("traceparent"))
            .map(|(_, v)| v.clone());
        let tracestate = request
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("tracestate"))
            .map(|(_, v)| v.clone());

        // The call args are written to the child's stdin as JSON.
        let request_stdin = serde_json::to_vec(&arguments).unwrap_or_default();

        let exec = ExecRequest {
            command: profile.command.clone(),
            args: resolved_args,
            stdin: request_stdin,
            timeout_ms: profile.timeout_ms,
            max_output_bytes: profile.max_output_bytes,
            traceparent,
            tracestate,
        };
        // The process work is blocking (std::process + reader threads +
        // a poll loop), so run it off the async worker.
        let exec_result = tokio::task::spawn_blocking(move || run_command(exec))
            .await
            .map_err(|e| BackendError::Transport {
                message: format!("command exec task join failed: {e}"),
            })?;

        let envelope = match exec_result {
            Ok(result) => {
                let (stdout_json, stdout_json_parse_error, stdout_json_validation_error) =
                    parse_and_validate_json_stdout(&result.stdout, profile.require_json_stdout);
                let command_errors =
                    collect_command_call_errors(&result, stdout_json_validation_error);
                build_command_envelope(
                    &tool_name,
                    backend_name,
                    &profile,
                    &arguments,
                    Some(&result),
                    stdout_json.as_ref(),
                    stdout_json_parse_error.as_deref(),
                    &command_errors,
                    None,
                )
            }
            Err(error) => {
                let command_error = command_execution_error(&error);
                build_command_envelope(
                    &tool_name,
                    backend_name,
                    &profile,
                    &arguments,
                    None,
                    None,
                    None,
                    std::slice::from_ref(&command_error),
                    Some(&error),
                )
            }
        };

        Ok(envelope_response(envelope))
    }

    fn audit_metadata(&self, _backend_name: &str) -> serde_json::Map<String, serde_json::Value> {
        let mut map = serde_json::Map::new();
        map.insert("command.transport".to_owned(), serde_json::json!("plugin"));
        map
    }
}

fn envelope_response(envelope: Value) -> BackendResponse {
    let payload = serde_json::to_vec(&envelope).unwrap_or_else(|_| b"{}".to_vec());
    BackendResponse {
        payload,
        truncated: false,
    }
}

/// Build an [`ExprContext`] for one call — identity claims propagate to
/// `$context.*`, args to `$arguments`. `$env.*` is empty (the gateway
/// resolves env vars at config-load time). Mirrors net-core's
/// `build_expr_context`; duplicated here so the command crate carries no
/// networking dependency.
fn build_expr_context(arguments: &Value, tool_name: &str, request: &BackendRequest) -> ExprContext {
    let mut ctx = ExprRequestContext {
        session_id: request.session_id.clone(),
        ..ExprRequestContext::default()
    };
    if let Some(identity) = request.identity.as_ref() {
        ctx.principal_id = identity.subject_id.clone();
        ctx.trust_level = identity.trust_level.clone();
        ctx.auth_provider = identity.auth_provider.clone();
        ctx.transport = identity.kind.clone();
        ctx.roles = identity.roles.clone();
        ctx.groups = identity.groups.clone();
        ctx.scopes = identity.scopes.clone();
        ctx.attributes = identity.attributes.clone();
    }
    ExprContext {
        arguments: arguments.clone(),
        tool_name: tool_name.to_owned(),
        context: ctx,
        steps: None,
        env: Arc::new(HashMap::new()),
    }
}

// ---------------------------------------------------------------------------
// Process execution
// ---------------------------------------------------------------------------

struct ExecRequest {
    command: String,
    args: Vec<String>,
    stdin: Vec<u8>,
    timeout_ms: u64,
    max_output_bytes: usize,
    traceparent: Option<String>,
    tracestate: Option<String>,
}

#[derive(Debug, Clone)]
struct CommandExecutionResult {
    exit_code: Option<i32>,
    success: bool,
    timed_out: bool,
    stdout: String,
    stderr: String,
    stdout_truncated: bool,
    stderr_truncated: bool,
    read_error: Option<String>,
    duration_ms: u128,
}

/// Spawn the configured process, write `stdin`, capture stdout/stderr
/// under the byte limit, and enforce the wall-clock timeout (kill on
/// expiry). Blocking — call from `spawn_blocking`.
fn run_command(req: ExecRequest) -> Result<CommandExecutionResult, String> {
    let started_at = Instant::now();
    let mut command = Command::new(&req.command);
    command
        .args(&req.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(tp) = &req.traceparent {
        command.env("TRACEPARENT", tp);
    }
    if let Some(ts) = &req.tracestate {
        command.env("TRACESTATE", ts);
    }

    let mut child = command.spawn().map_err(|e| e.to_string())?;

    // Drain stdout/stderr and feed stdin all CONCURRENTLY. Writing the
    // full stdin inline before spawning the readers deadlocks a
    // filter-style child (cat/jq/…): once it emits > ~64KB to a stdout
    // pipe nobody is draining, the child blocks on stdout and stops
    // reading stdin, so the parent's stdin write blocks too. So the
    // stdin write goes on its own thread; the timeout loop below can
    // then kill the child (which unblocks a stuck stdin write via a
    // broken pipe) even if the child never reads stdin.
    let stdin = child.stdin.take();
    let stdout = child.stdout.take().expect("stdout piped");
    let stderr = child.stderr.take().expect("stderr piped");
    let stdout_handle = spawn_limited_reader(stdout, req.max_output_bytes);
    let stderr_handle = spawn_limited_reader(stderr, req.max_output_bytes);
    let stdin_handle = stdin.map(|mut stdin| {
        let payload = req.stdin;
        thread::spawn(move || {
            // Best-effort: a child that closes stdin early (or never
            // reads it) shouldn't fail the call with a broken pipe.
            let _ = stdin.write_all(&payload);
            // Drop `stdin` here to close the pipe (EOF for the child).
        })
    });

    let timeout = Duration::from_millis(req.timeout_ms);
    let mut timed_out = false;
    let exit_status = loop {
        match child.try_wait().map_err(|e| e.to_string())? {
            Some(status) => break status,
            None => {
                if started_at.elapsed() >= timeout {
                    timed_out = true;
                    let _ = child.kill();
                    break child.wait().map_err(|e| e.to_string())?;
                }
                thread::sleep(Duration::from_millis(10));
            }
        }
    };

    let stdout_result = stdout_handle.join().expect("stdout reader joined");
    let stderr_result = stderr_handle.join().expect("stderr reader joined");
    // The child has exited (or been killed), so any blocked stdin write
    // has now hit a broken pipe and returned; join the writer thread.
    if let Some(handle) = stdin_handle {
        let _ = handle.join();
    }
    let read_error = stdout_result.error.or(stderr_result.error);

    Ok(CommandExecutionResult {
        exit_code: exit_status.code(),
        success: exit_status.success() && !timed_out,
        timed_out,
        stdout: String::from_utf8_lossy(&stdout_result.bytes).to_string(),
        stderr: String::from_utf8_lossy(&stderr_result.bytes).to_string(),
        stdout_truncated: stdout_result.truncated,
        stderr_truncated: stderr_result.truncated,
        read_error,
        duration_ms: started_at.elapsed().as_millis(),
    })
}

struct LimitedReadResult {
    bytes: Vec<u8>,
    truncated: bool,
    error: Option<String>,
}

fn spawn_limited_reader<R: Read + Send + 'static>(
    mut reader: R,
    limit: usize,
) -> thread::JoinHandle<LimitedReadResult> {
    thread::spawn(move || {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        let mut truncated = false;
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => {
                    if bytes.len() < limit {
                        let remaining = limit - bytes.len();
                        let copy_len = remaining.min(count);
                        bytes.extend_from_slice(&buffer[..copy_len]);
                        if count > copy_len {
                            truncated = true;
                        }
                    } else {
                        truncated = true;
                    }
                }
                Err(error) => {
                    return LimitedReadResult {
                        bytes,
                        truncated,
                        error: Some(error.to_string()),
                    };
                }
            }
        }
        LimitedReadResult {
            bytes,
            truncated,
            error: None,
        }
    })
}

// ---------------------------------------------------------------------------
// Error classification + envelope
// ---------------------------------------------------------------------------

/// Operator-facing per-call error returned in the envelope's
/// `downstreamError` slot. Mirrors the shape the gateway built inline.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct DownstreamCommandError {
    kind: String,
    code: String,
    message: String,
    retryable: bool,
    suggested_action: String,
    details: Value,
}

fn parse_and_validate_json_stdout(
    stdout: &str,
    require_json_stdout: bool,
) -> (
    Option<Value>,
    Option<String>,
    Option<DownstreamCommandError>,
) {
    if stdout.trim().is_empty() {
        if require_json_stdout {
            return (
                None,
                None,
                Some(invalid_json_stdout_error("stdout was empty")),
            );
        }
        return (None, None, None);
    }
    match serde_json::from_str::<Value>(stdout) {
        Ok(value) => (Some(value), None, None),
        Err(error) => {
            let parse_error = error.to_string();
            let validation_error = if require_json_stdout {
                Some(invalid_json_stdout_error(&parse_error))
            } else {
                None
            };
            (None, Some(parse_error), validation_error)
        }
    }
}

fn collect_command_call_errors(
    result: &CommandExecutionResult,
    stdout_json_validation_error: Option<DownstreamCommandError>,
) -> Vec<DownstreamCommandError> {
    let mut errors = Vec::new();
    if result.timed_out {
        errors.push(command_timeout_error());
    }
    if !result.timed_out && !result.success {
        errors.push(command_non_zero_exit_error(result.exit_code));
    }
    if let Some(read_error) = &result.read_error {
        errors.push(command_read_error(read_error));
    }
    if let Some(stdout_json_validation_error) = stdout_json_validation_error {
        errors.push(stdout_json_validation_error);
    }
    errors
}

fn command_timeout_error() -> DownstreamCommandError {
    DownstreamCommandError {
        kind: "timeout".to_owned(),
        code: "mcpg.downstream_command.timeout".to_owned(),
        message: "Downstream command execution timed out before completion.".to_owned(),
        retryable: false,
        suggested_action: "inspect_command_runtime_and_review_idempotency_before_retry".to_owned(),
        details: serde_json::json!({}),
    }
}

fn command_non_zero_exit_error(exit_code: Option<i32>) -> DownstreamCommandError {
    DownstreamCommandError {
        kind: "non_zero_exit".to_owned(),
        code: "mcpg.downstream_command.non_zero_exit".to_owned(),
        message: "Downstream command execution completed with a non-zero exit status.".to_owned(),
        retryable: false,
        suggested_action: "inspect_command_stderr_and_exit_status".to_owned(),
        details: serde_json::json!({ "exitCode": exit_code }),
    }
}

fn command_read_error(read_error: &str) -> DownstreamCommandError {
    DownstreamCommandError {
        kind: "read_error".to_owned(),
        code: "mcpg.downstream_command.read_error".to_owned(),
        message: "Downstream command execution completed, but stdout/stderr capture failed."
            .to_owned(),
        retryable: false,
        suggested_action: "inspect_command_output_capture_path".to_owned(),
        details: serde_json::json!({ "readError": read_error }),
    }
}

fn invalid_json_stdout_error(parse_error: &str) -> DownstreamCommandError {
    DownstreamCommandError {
        kind: "invalid_json_stdout".to_owned(),
        code: "mcpg.downstream_command.invalid_json_stdout".to_owned(),
        message: "Downstream command JSON call required valid JSON on stdout, but stdout was not valid JSON.".to_owned(),
        retryable: false,
        suggested_action: "inspect_command_stdout_contract".to_owned(),
        details: serde_json::json!({ "parseError": parse_error }),
    }
}

fn command_execution_error(error: &str) -> DownstreamCommandError {
    DownstreamCommandError {
        kind: "execution_error".to_owned(),
        code: "mcpg.downstream_command.execution_error".to_owned(),
        message: "Downstream command execution failed before a valid result was produced."
            .to_owned(),
        retryable: false,
        suggested_action: "inspect_command_invocation_and_runtime_environment".to_owned(),
        details: serde_json::json!({ "error": error }),
    }
}

/// Build the command structured-content envelope. Carries the process
/// outcome plus the shared `downstreamError` slot (first error) +
/// `downstreamErrors` (all) the gateway reads for `is_error`.
#[allow(clippy::too_many_arguments)]
fn build_command_envelope(
    tool_name: &str,
    backend_name: &str,
    profile: &CommandProfile,
    request_arguments: &Value,
    result: Option<&CommandExecutionResult>,
    stdout_json: Option<&Value>,
    stdout_json_parse_error: Option<&str>,
    command_errors: &[DownstreamCommandError],
    error: Option<&str>,
) -> Value {
    let primary = command_errors.first();
    serde_json::json!({
        "toolName": tool_name,
        "profile": backend_name,
        "requestKind": "json_stdin",
        "request": {
            "kind": "json_stdin",
            "arguments": request_arguments,
            "body": request_arguments,
        },
        "response": result.map(|r| serde_json::json!({
            "durationMs": r.duration_ms,
            "exitCode": r.exit_code,
            "success": r.success,
            "timedOut": r.timed_out,
            "stdout": r.stdout,
            "stderr": r.stderr,
            "stdoutTruncated": r.stdout_truncated,
            "stderrTruncated": r.stderr_truncated,
            "readError": r.read_error,
            "json": stdout_json,
            "jsonParseError": stdout_json_parse_error,
        })),
        "command": profile.command,
        "requireJsonStdout": profile.require_json_stdout,
        "timeoutMs": profile.timeout_ms,
        "maxOutputBytes": profile.max_output_bytes,
        "error": error,
        "downstreamError": primary
            .map(|e| serde_json::to_value(e).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),
        "downstreamErrors": serde_json::to_value(command_errors).unwrap_or(Value::Null),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(args: Value) -> BackendRequest {
        BackendRequest {
            payload: serde_json::to_vec(&args).unwrap(),
            headers: vec![],
            request_id: "rq-1".into(),
            session_id: None,
            identity: None,
            idempotency: None,
        }
    }

    #[test]
    fn binding_plugin_kind_is_command() {
        assert_eq!(CommandBackendPlugin::new().kind(), "command");
    }

    #[test]
    fn manifest_advertises_first_party_id() {
        assert_eq!(
            CommandBackendPlugin::new().manifest().id,
            "dev.mcpg.backend.command"
        );
    }

    #[tokio::test]
    async fn register_rejects_empty_command() {
        let plugin = CommandBackendPlugin::new();
        let spec = serde_json::json!({ "command": "" });
        let err = plugin
            .register_profile("t", &spec, mcpg_plugin_protocol::noop_backend_host())
            .await
            .expect_err("empty command rejected");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    #[tokio::test]
    async fn echo_command_round_trips_stdout() {
        let plugin = CommandBackendPlugin::new();
        // `cat` echoes the JSON stdin back to stdout.
        let spec = serde_json::json!({
            "command": "cat",
            "args": [],
            "require_json_stdout": true,
        });
        plugin
            .register_profile("c", &spec, mcpg_plugin_protocol::noop_backend_host())
            .await
            .expect("register");
        let resp = plugin
            .execute("c", req(serde_json::json!({"hello": "world"})))
            .await
            .expect("execute");
        let env: Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(env["response"]["exitCode"], 0);
        assert_eq!(env["response"]["success"], true);
        assert!(env["downstreamError"].is_null());
        assert_eq!(env["response"]["json"]["hello"], "world");
    }

    #[tokio::test]
    async fn non_zero_exit_flags_downstream_error() {
        let plugin = CommandBackendPlugin::new();
        let spec = serde_json::json!({
            "command": "sh",
            "args": ["-c", "exit 3"],
        });
        plugin
            .register_profile("c", &spec, mcpg_plugin_protocol::noop_backend_host())
            .await
            .expect("register");
        let resp = plugin
            .execute("c", req(serde_json::json!({})))
            .await
            .expect("execute");
        let env: Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(env["response"]["exitCode"], 3);
        assert!(!env["downstreamError"].is_null());
        assert_eq!(env["downstreamError"]["kind"], "non_zero_exit");
    }

    #[tokio::test]
    async fn large_filter_output_does_not_deadlock() {
        // Regression for the pipe deadlock: `cat` echoes a >64KB stdin to
        // stdout. If stdin were written in full before the stdout reader
        // was spawned, the child would block on a full stdout pipe while
        // the parent blocked on a full stdin pipe. The fix (concurrent
        // stdin writer + readers) must let this round-trip.
        let plugin = CommandBackendPlugin::new();
        let spec = serde_json::json!({
            "command": "cat",
            "args": [],
            "max_output_bytes": 1_048_576usize,
            "timeout_ms": 10_000u64,
        });
        plugin
            .register_profile("c", &spec, mcpg_plugin_protocol::noop_backend_host())
            .await
            .expect("register");
        // ~256KB JSON payload — well over the ~64KB kernel pipe buffer.
        let big = "x".repeat(256 * 1024);
        let resp = plugin
            .execute("c", req(serde_json::json!({ "data": big })))
            .await
            .expect("execute");
        let env: Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(
            env["response"]["timedOut"], false,
            "must not time out / deadlock"
        );
        assert_eq!(env["response"]["exitCode"], 0);
        assert!(env["downstreamError"].is_null());
        assert!(
            env["response"]["stdout"].as_str().unwrap().len() > 64 * 1024,
            "cat should have echoed the full large payload"
        );
    }

    #[tokio::test]
    async fn arg_cel_template_resolves_against_arguments() {
        let plugin = CommandBackendPlugin::new();
        // echo the templated arg to stdout (non-JSON; require_json off).
        let spec = serde_json::json!({
            "command": "printf",
            "args": ["%s", "${arguments.name}"],
        });
        plugin
            .register_profile("c", &spec, mcpg_plugin_protocol::noop_backend_host())
            .await
            .expect("register");
        let resp = plugin
            .execute("c", req(serde_json::json!({"name": "templated"})))
            .await
            .expect("execute");
        let env: Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(env["response"]["stdout"], "templated");
        assert!(env["downstreamError"].is_null());
    }

    /// SECURITY: command never resolves `cred://` (it is handed a no-op
    /// host and never calls it) and its request-time CEL context has an
    /// EMPTY `$env` (env is config-load-only). So a `cred://` or
    /// `${env.X}` smuggled through a request argument that the operator
    /// templates into a command arg reaches the child process as a
    /// VERBATIM LITERAL — never a resolved secret. (`printf %s` echoes its
    /// arg to stdout, so we read back exactly what the child received.)
    #[tokio::test]
    async fn request_injected_cred_and_env_markers_reach_child_as_literals() {
        // A real, set env var the attacker is trying to read via ${env.X}.
        // SAFETY: nextest isolates each test in its own process; unique name.
        unsafe {
            std::env::set_var("MCPG_CMD_INJECT_SECRET", "TOP_SECRET_CMD_VALUE");
        }

        let plugin = CommandBackendPlugin::new();
        let spec = serde_json::json!({
            "command": "printf",
            "args": ["%s", "${arguments.payload}"],
        });
        plugin
            .register_profile("c", &spec, mcpg_plugin_protocol::noop_backend_host())
            .await
            .expect("register");

        // (1) cred:// smuggled in a request argument → literal, not resolved.
        let resp = plugin
            .execute(
                "c",
                req(serde_json::json!({ "payload": "cred://static/secret" })),
            )
            .await
            .expect("execute");
        let env: Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(
            env["response"]["stdout"], "cred://static/secret",
            "command must pass a request-injected cred:// through as a verbatim literal"
        );

        // (2) ${env.X} smuggled in a request argument → literal, not expanded.
        let resp = plugin
            .execute(
                "c",
                req(serde_json::json!({ "payload": "${env.MCPG_CMD_INJECT_SECRET}" })),
            )
            .await
            .expect("execute");
        let env: Value = serde_json::from_slice(&resp.payload).unwrap();
        let stdout = env["response"]["stdout"].as_str().unwrap_or_default();
        assert!(
            !stdout.contains("TOP_SECRET_CMD_VALUE"),
            "SECURITY: command expanded a request-injected ${{$env.X}} into the child (stdout='{stdout}')"
        );
        assert_eq!(
            stdout, "${env.MCPG_CMD_INJECT_SECRET}",
            "request-injected ${{$env.X}} must reach the child as a verbatim literal"
        );

        unsafe {
            std::env::remove_var("MCPG_CMD_INJECT_SECRET");
        }
    }
}
