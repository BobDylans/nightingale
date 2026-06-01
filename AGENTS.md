# AGENTS.md

## Build

```bash
make              # prebuild (download fe + statik embed) + go build → ./n9e
make build        # go build only (no prebuild)
make build-edge   # n9e-edge (distributed alert engine)
make build-cli    # n9e-cli (DB upgrade from v5)
```

`make prebuild` downloads frontend assets from `github.com/n9e/fe` into `./pub`, then runs `statik` to embed them as `front/statik/statik.go`. The `pub/` dir and `front/statik/statik.go` are gitignored.

Version is injected via ldflags: `-X github.com/ccfos/nightingale/v6/pkg/version.Version=...`

## Test

```bash
go test ./...               # all tests (no race)
go test -race ./...          # with race detection
go test -cover ./...         # with coverage
go test ./alert/eval -run Test_originalJoin  # single test
```

No linter, formatter, or typecheck config in the repo (only `.typos.toml` for the `typos` tool). CI runs GoReleaser on v* tags only — no PR CI.

## Architecture

**Single-module Go monorepo** (`github.com/ccfos/nightingale/v6`). Five binaries in `cmd/`, all share the same `go.mod`.

### Key packages

| Package | Role |
|---------|------|
| `center/` | HTTP API, SSO, biz groups, dashboards — wiring in `center.Initialize()` |
| `alert/` | Rule eval, muting, dispatch, pipeline, sender, event queue |
| `pushgw/` | Metrics ingestion (Remote Write, OpenTSDB, Datadog, Falcon), label rewrite, Kafka |
| `models/` | GORM models for all entities |
| `memsto/` | In-memory caches synced from DB (hot read path) |
| `datasource/` | Query engine per DS type (Prom, ES, ClickHouse, MySQL, PG, etc.) |
| `storage/` | DB (`storage.New`) and Redis (`storage.NewRedis`) abstractions |
| `conf/` | TOML config loading (via `koding/multiconfig`) + crypto-key decryption |
| `cron/` | Scheduled cleanup of notification records, pipeline executions |
| `aiagent/` | AI agent subsystem (A2A protocol, LLM, MCP, skill-based chat) |
| `integrations/` | ~80 pre-built monitoring integration definitions |

### Configuration

- Default: SQLite (`n9e.db`) + miniredis — zero-dependency dev mode
- Production: MySQL/PostgreSQL + Redis + external TSDB (VictoriaMetrics recommended)
- Config dir: `-configs` flag or `N9E_CONFIGS` env var (default `etc/`)

## Conventions

- `center.Initialize(configDir, cryptoKey)` is the main wiring function — reads all center.go init to trace the system
- All `memsto` caches follow the same pattern: `New*Cache(ctx, syncStats, ...)` + periodic DB sync
- `toolkits/pkg` is heavily used for utilities (runner, logger, HTTP client, ORM helpers)

## Edge cases

- The `n9e-cli` binary only handles DB migration from v5 — not a general-purpose CLI
- `event` package exists under `deprecated/` — do not import it
- The `docker/compose-*` dirs contain various deployment compose files; the primary Docker image is built via `docker/Dockerfile.goreleaser`
