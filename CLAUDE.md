# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Nightingale (n9e)** — Cloud-native monitoring alerting engine by Flashcat/CCF. Written in Go (Gin framework). v6.x.

It focuses on alerting: connects to existing data sources (Prometheus, VictoriaMetrics, ES, ClickHouse, etc.), evaluates alert rules, and sends notifications via 20+ channels.

## Build & Test Commands

```bash
# Build all (downloads frontend assets first)
make

# Build individual binaries
make build          # n9e - center server (everything)
make build-edge     # n9e-edge - distributed alert engine
make build-alert    # n9e-alert - standalone alert engine
make build-pushgw   # n9e-pushgw - standalone push gateway
make build-cli      # n9e-cli - CLI tools (DB upgrade)

# Run
make run            # Run n9e in background

# Test
go test ./...               # All tests
go test -race ./...          # With race detection
go test -cover ./...         # With coverage
go test ./models -run TestXxx  # Single test
```

## Project Architecture

### Binaries (in `cmd/`)

| Binary | Entry | Purpose |
|--------|-------|---------|
| `n9e` | `cmd/center/main.go` | **All-in-one** — merges center + alert + pushgw in one process. Most common deployment. |
| `n9e-edge` | `cmd/edge/main.go` | **Distributed alert engine** — runs alerts locally for remote data centers with poor connectivity to central server. |
| `n9e-alert` | `cmd/alert/main.go` | **Standalone alert engine** — alert evaluation + dispatch only. |
| `n9e-pushgw` | `cmd/pushgw/main.go` | **Standalone push gateway** — receives metrics via Remote Write / OpenTSDB / Datadog / OpenFalcon, writes to TSDB. |
| `n9e-cli` | `cmd/cli/main.go` | **CLI tool** — database upgrade from v5. |

### Core Packages

- **`center/`** — HTTP API, SSO/auth, integration management, business groups, dashboards
- **`alert/`** — Alert engine: rule evaluation (`eval/`), muting (`mute/`), dispatching (`dispatch/`), pipeline processing (`pipeline/`), notification sending (`sender/`), event queue (`queue/`), alerting stats (`astats/`)
- **`pushgw/`** — Metrics ingestion: Remote Write, OpenTSDB, Datadog, OpenFalcon protocol routers; label rewriting; Kafka output; TSDB writers
- **`models/`** — GORM data models for all entities (alert rules, mute rules, users, targets, dashboards, notify channels, etc.)
- **`memsto/`** — In-memory caches syncing from DB (target cache, alert rule/mute cache, user cache, etc.) — the hot read path
- **`datasource/`** — Query engine per data source type (Prometheus, VictoriaLogs, Elasticsearch, ClickHouse, MySQL, PostgreSQL, TDengine, Doris, OpenSearch)
- **`storage/`** — DB (GORM: SQLite/MySQL/PostgreSQL) and Redis abstractions
- **`conf/`** — TOML config loading, crypto-key decryption
- **`pkg/`** — Shared utilities (HTTP client, ORM helpers, PromQL parser, i18n, LDAP, OAuth2/OIDC, etc.)
- **`cron/`** — Scheduled cleanup jobs (notification records, pipeline executions)
- **`aiagent/`** — AI agent subsystem: A2A protocol, LLM integration, MCP server integration, skill-based chat, streaming
- **`integrations/`** — Pre-built monitoring integration definitions (OS, middleware, DB — ~80+ components)
- **`dscache/`** — Datasource runtime cache (schema, index patterns)
- **`dumper/`** — Debug info dumper
- **`dskit/`** — Data source kit (MySQL helpers for integration stores)

### Data Flow

```
Categraf / Agent → Push Gateway (pushgw/) → TSDB (VictoriaMetrics/Prometheus etc.)
                                                      ↑
User configures alert rules → models/alert_rule.go ←→ memsto/ (in-memory cache)
                                       ↓
Alert eval (alert/eval/) ← queries → Datasource (datasource/prom/)
         ↓
    Event matched? → Mute check (alert/mute/)
         ↓
    Alert pipeline (alert/pipeline/) → Event dispatch (alert/dispatch/)
         ↓
    Notification sender (alert/sender/) → 20+ channels (DingTalk, Slack, Email, SMS, Phone, etc.)
```

### Key Config

`etc/config.toml` — TOML format. Sections:
- `[Global]` — RunMode
- `[Log]` — Log dir, level, output
- `[HTTP]` — Listen address, TLS, CORS, auth methods (JWT/Proxy/Token)
- `[DB]` — SQLite (default), MySQL, or PostgreSQL
- `[Redis]` — Required for JWT, pub/sub. Supports standalone/cluster/sentinel/miniredis
- `[Alert]` — Heartbeat, alerting settings
- `[Center]` — Metrics YAML, i18n, anonymous access settings
- `[Pushgw]` — Label rewrite, writers (TSDB targets), Kafka writers
- `[Ibex]` — Built-in script execution engine

### Deployment

- Default: single binary `n9e` with SQLite + miniredis (zero-dependency dev mode)
- Production: MySQL/PostgreSQL + Redis + external TSDB (VictoriaMetrics recommended)
- Edge: `n9e-edge` runs in remote datacenters, connects back to center via heartbeat
- Docker: `docker/Dockerfile.goreleaser` — goreleaser-based multi-arch builds
- Docker Compose: `docker/compose-host-network/` etc. for various deployment modes
