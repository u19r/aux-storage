# config

Runtime configuration types needed by aux-storage services and tests.

This is a slim configuration crate for storage, queue, stream, logging, metrics, and background maintenance settings.

Keep new settings tied to a service or library in this repository. Do not add downstream application configuration here.

## Boundary

`crates/config` owns launch configuration assembly for aux-storage binaries.
`storage-api` calls `StorageApiLaunchConfig::from_args(std::env::args_os())`
and receives raw launch inputs plus a validated effective config.

The pipeline is:

```text
parse args -> load JSON or defaults -> apply top-level flags -> apply --overrides -> interpolate strings -> schema validate -> semantic validate -> return launch config
```

## Schema

Generate the canonical schema with:

```bash
cargo run -p config --bin config -- --write-schema crates/config/config.schema.json
```

Config documents may reference it with `$schema`:

```json
{
  "$schema": "https://raw.githubusercontent.com/u19r/aux-storage/refs/heads/main/crates/config/config.schema.json?token=GHSAT0AAAAAADK4QYGE6FCAG4LATCSLN4UI2QGCFVA",
  "features": {
    "backends": {
      "sqlite": { "db_path": "./data/storage.sqlite" }
    }
  }
}
```

The committed schema is generated from `src/model.rs` with small documented
patches in `src/schema.rs` for constraints that the derive cannot express.

## Overrides

Top-level flags map to canonical JSON config paths. `--overrides` has final
precedence and accepts comma-separated `path=value` assignments:

```bash
storage-api --config config.json \
  --port 3000 \
  --overrides 'http.bind_addr="127.0.0.1:9000",features.runtime.enable_background_workers=false'
```

Repeat `--overrides` to avoid fragile shell quoting. Escape literal commas or
equals in string values with `\,` and `\=`.

## Storage admission

The `features.storage_admission` object configures foreground provider
admission independently for each storage connection. Defaults are enabled,
20,000 sustainable requests per second, 5 ms latency, a foreground window of
4..=1024 permits, a four-permit control reserve, a 256-entry queue, and a
25 ms maximum queue wait. The controller uses the throughput/latency estimate
as its initial window and returns a retryable overload response when the queue
is full or the wait expires.

The same fields are available as `--storage-admission-*` flags. Environment
variables use `AUX_STORAGE_ADMISSION_*`; the compatibility shorthand
`AUX_STORAGE_INITIAL_SUSTAINABLE_THROUGHPUT_RPS` is also accepted; when both
throughput names are set, the canonical admission name wins. Explicit
`--overrides` win over flags, which win over environment values and file data.

## Interpolation

All JSON string values support `${ENV}` and `file::path::`. Resolver expressions
can be mixed with literal text and nested. Relative `file::path::` references are
resolved from the config file directory.

Object keys are intentionally not interpolated. Missing environment variables
and unreadable files are hard errors.

Sensitive fields include database DSNs, remote credentials, and replication
service tokens. `features.metrics.enabled` defaults to `true` and controls the
storage-api `/metrics` endpoint. Set
`features.metrics.prometheus.bearer_token` to require an
`Authorization: Bearer <token>` header. Avoid logging effective config unless it
is redacted.

## Extension Rules

Add fields only for storage, queue, stream, provider, service launch, tracing,
metrics, and directly supporting background maintenance behavior. JSON-facing
types should derive `Serialize`, `Deserialize`, and `JsonSchema`, deny unknown
fields unless they are intentional dictionaries, and use typed validation for
constraints that schema cannot express.
