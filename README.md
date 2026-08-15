# Command Binding — `dev.mcpg.backend.command`

> class `backend` · `native` · package `mcpg-plugin-backend-command` · artifact `libmcpg_plugin_backend_command.so` · Apache-2.0

Backend binding plugin for the MCPG gateway that turns a local
executable into an MCP surface. It dispatches a call by spawning the
operator-configured process, writing the call arguments to the child's
stdin as a single JSON document, and capturing stdout and stderr under
byte and wall-clock limits. Reach for it to expose an existing CLI,
script, or batch job as an MCP tool without writing an HTTP wrapper
around it — unlike the network bindings it links no HTTP client and
opens no sockets.

## What it does
- Spawns `command` with the resolved `args[]`, feeds the call arguments
  as JSON on stdin, and returns captured stdout in a structured
  envelope.
- Resolves each entry of `args[]` as a CEL template against the call's
  `${arguments.*}` and `${context.*}`; the `command` path itself is
  structural and is never templated.
- Enforces a wall-clock `timeout_ms` (the child is killed on expiry) and
  caps captured stdout and stderr at `max_output_bytes` each, flagging
  truncation in the envelope.
- Optionally requires stdout to parse as JSON (`require_json_stdout`)
  and surfaces the parsed document alongside the raw text.
- Classifies a timeout, a non-zero exit, an output-capture failure, or a
  failed JSON requirement into the envelope's `downstreamError` slot,
  which the gateway reads to mark the MCP result as an error.
- Propagates W3C trace context to the child as the `TRACEPARENT` and
  `TRACESTATE` environment variables.
- Declares no `required_capabilities` — it needs neither
  `network_outbound` nor `transport_listen`.

## Configuration
The `plugins:` entry loads the cdylib and takes no `config:` block; the
per-call configuration lives in each binding's `backend:` block, keyed by
the `kind: command` discriminator.

```yaml
plugins:
  - id: dev.mcpg.backend.command
    class: backend
    kind: native
    source:
      path: ./plugins/libmcpg_plugin_backend_command.so

mcp:
  capabilities:
    tools:
      - name: docs.render
        description: Render a document with the local renderer CLI.
        backend:
          kind: command
          command: /usr/local/bin/render
          args: ["--format", "json", "--id", "${arguments.id}"]
          timeout_ms: 5000
          max_output_bytes: 65536
          require_json_stdout: true
```

| Field | Type | Default | Description |
|---|---|---|---|
| `command` | string | — (required) | Executable path. Structural: never CEL-templated, and rejected when empty. |
| `args` | `[string]` | `[]` | Arguments. Each entry is a CEL template resolved per call against `${arguments.*}` / `${context.*}`. |
| `timeout_ms` | u64 | `5000` | Wall-clock budget; the child is killed on expiry. Must be greater than 0. |
| `max_output_bytes` | usize | `65536` | Per-stream capture cap for stdout and stderr. Must be greater than 0. |
| `require_json_stdout` | bool | `false` | When true, stdout that is empty or not JSON-parseable becomes a downstream error. |

Arg templates are compiled at registration time, so a malformed CEL
expression fails the binding at boot rather than on the first call. A
plain literal (no `${…}`) skips the CEL engine entirely.

## Security
The command path is the security boundary, and the operator's config
fixes it: it is never derived from a request. Only `args[]` interpolate
caller-controlled values, and that interpolation is value substitution —
it does not re-enter a shell, so a caller cannot append arguments or
chain a second command.

This plugin never resolves credentials. It is handed a host it does not
call, and its request-time CEL context carries an empty `$env` map
because environment references resolve once at config load. A `cred://`
URI or a `${env.X}` token smuggled through a request argument therefore
reaches the child as a verbatim literal, never as a resolved secret.

## Response envelope
`execute` returns a JSON document rather than raw stdout. Alongside
`toolName` and `profile` it carries a `response` object with `exitCode`,
`success`, `timedOut`, `stdout`, `stderr`, `stdoutTruncated`,
`stderrTruncated`, `readError`, `durationMs`, and — when stdout parsed —
`json`. The `downstreamError` slot holds the first classified error
(`timeout`, `non_zero_exit`, `read_error`, `invalid_json_stdout`,
`execution_error`), with the full list under `downstreamErrors`. Each
error carries a stable `code`, a `retryable` flag, and a
`suggestedAction` string.

## MCP surfaces & composition
The binding is declared per capability under `mcp.capabilities.*`; the
same `backend:` block shape works on every surface.

### As a pipeline step
`kind: command` is pipeline-capable. Step keys other than `id` and
`input_transform` flatten into the spec. A step's `input_transform` is
where earlier step results are addressable; whatever it evaluates to
becomes the `${arguments.*}` this binding's arg templates resolve
against.

```yaml
backend:
  kind: pipeline
  steps:
    - kind: command
      id: render
      command: /usr/local/bin/render
      args: ["--id", "${arguments.id}"]
```

### As a resource
```yaml
mcp:
  capabilities:
    resources:
      - name: report.latest
        description: The latest generated report.
        uri: "report://latest"
        mime_type: application/json
        backend:
          kind: command
          command: /usr/local/bin/report
          args: ["--latest"]
```

### As a resource template
Variables captured from `uri_template` arrive in `arguments` under their
declared names, so they interpolate into `args` like any other argument.

```yaml
mcp:
  capabilities:
    resource_templates:
      - name: report.by_day
        description: A generated report for one day.
        uri_template: "report://{day}"
        mime_type: application/json
        backend:
          kind: command
          command: /usr/local/bin/report
          args: ["--day", "${arguments.day}"]
```

### As a prompt
```yaml
mcp:
  capabilities:
    prompts:
      - name: review.checklist
        description: Build a review checklist for a repository.
        prompt_arguments:
          - name: repo
            required: true
        backend:
          kind: command
          command: /usr/local/bin/checklist
          args: ["--repo", "${arguments.repo}"]
```

### Schemas & annotations
Every binding accepts the MCP descriptor fields as siblings of
`backend:` — `title`, `input_schema`, `output_schema`, `icons`, and
`annotations` (`read_only`, `destructive`, `idempotent`, `open_world`).
A sibling `retry:` block (`max_attempts` default `3`,
`initial_backoff_ms` default `200`, `retry_on_transport_error` default
true) governs gateway-side retries, and `governance:` carries the trust
floor and CEL authorization for the surface.

## Build
Default feature set is OFF (avoids `mcpg_plugin_register` linker
collisions in the workspace build); opt in to the cdylib:

```bash
cargo build -p mcpg-plugin-backend-command --features cdylib-export --release   # → target/release/libmcpg_plugin_backend_command.so
```

Releases publish a platform-agnostic OCI artifact, so a `plugins:` entry
can set `source.oci` to
`ghcr.io/mcpg-dev/source-code/plugins/backend-command:protocol-1` instead
of `source.path` and let the gateway resolve the right os/arch/libc
build for its host.

## Testing
```bash
cargo test -p mcpg-plugin-backend-command
```

The suite runs offline: it drives real subprocesses (`cat`, `sh`,
`printf`) to cover stdout round-tripping, non-zero exits, large-payload
pipe behaviour, and the credential/env literal-passthrough guarantees.

## Sign & load (production)
Sign the artifact, pin/verify via the entry's `signature:` block, and honour
revocations. See <https://mcpg.dev/docs/security/plugin-security>.

## See also
- Backend binding reference: <https://mcpg.dev/docs/reference/backends>
- Pipeline step kinds: <https://mcpg.dev/docs/reference/pipeline-steps>
- Full gateway config schema: <https://mcpg.dev/docs/reference/configuration>
- Network-backed siblings: `libs/plugins/backend/http`, `libs/plugins/backend/grpc`, `libs/plugins/backend/graphql`
