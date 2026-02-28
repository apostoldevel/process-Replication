[![ru](https://img.shields.io/badge/lang-ru-green.svg)](README.ru-RU.md)

Replication Server
-

**Process** for **Apostol CRM**[^crm].

Description
-

**Replication Server** is a background process module for the [Апостол (C++20)](https://github.com/apostoldevel/libapostol) framework. It synchronizes data between Apostol CRM nodes over HTTP REST with gzip compression. Designed for low-bandwidth channels (satellite links on maritime vessels).

Key characteristics:

* Written in C++20 using an asynchronous, non-blocking I/O model based on the **epoll** API.
* Connects to **PostgreSQL** via `libpq` using the `apibot` database role (helper connection pool).
* Authenticates to a remote master node via **OAuth2 `client_credentials`** using `FetchClient`.
* **NOTIFY-driven**: subscribes to PostgreSQL `LISTEN replication_cmd` for operator commands (mode/channel/sync).
* **Automatic sync**: heartbeat-driven with configurable intervals per channel type.
* **Priority-based filtering**: satellite sends only high-priority data, LAN sends everything.
* **Offline accumulation**: data is always captured and stored locally, synced when connectivity is available.
* **One HTTP round-trip per sync cycle**: slave sends its batch and receives the peer's batch in a single POST/response.

### Architecture

Replication Server follows the **ProcessModule** pattern introduced in apostol.v2:

```
Application
  └── ModuleProcess (generic process shell: signals, EventLoop, PgPool)
        └── ReplicationServer (ProcessModule: business logic only)
```

The process lifecycle (signal handling, crash recovery, PgPool setup, heartbeat timer) is managed by the generic `ModuleProcess` shell. `ReplicationServer` only contains the sync logic.

### Data flow

```
[Application] → INSERT/UPDATE/DELETE → [PostgreSQL WAL]
                                            │
[Publication: apostol_repl]                 │
[Slot: apostol_repl]  ◄────────────────────┘
      │
[ReplicationServer.drain_slot()]
      │  pg_logical_slot_get_changes() → wal2json
      ▼
[replication.outbox] ← priority from replication.list
      │
[sync()] ── FetchClient POST → remote /replication/sync
      │                            │
      │     ◄── response ──────────┘
      ▼
[apply_batch()] → INSERT/UPDATE/DELETE (DEFERRED constraints)
```

### Sync cycle

```
heartbeat (1s)
  └── refresh_token()            — remote OAuth2 via FetchClient
  └── drain_slot()               — always runs (even when paused)
  └── process_notify_queue()     — LISTEN "replication_cmd"
  └── if automatic && interval elapsed:
        └── start_sync()
              1. fetch_outbox(peer, channel, limit)  → collect local batch
              2. POST master/replication/sync         → send batch, receive peer batch
              3. apply_batch(source, entries)          → apply incoming entries
              4. ack(source, max_id)                  → update watermarks
              5. schedule_next_sync()                 → 5s if has_more, else interval
```

### Modes

| Mode | Description |
|------|-------------|
| `automatic` | Sync on heartbeat interval (default) |
| `paused` | Data accumulates in outbox but is not sent |
| `manual` | Sync only on explicit command via NOTIFY |

Modes can be switched at runtime via `NOTIFY replication_cmd '{"action":"mode","mode":"paused"}'`.

### Channels

| Channel | Interval | Priority filter | Tables sent |
|---------|----------|-----------------|-------------|
| `lan` | 30s | all (1-3) | Everything |
| `wifi` | 60s | high + medium (1-2) | Most tables |
| `satellite` | 300s | high only (1) | Critical data only |

All changes are always stored in the outbox. When the channel switches from satellite to LAN, accumulated medium and low priority entries are sent.

Database module
-

ReplicationServer is coupled to the **`replication`** module of [db-platform](https://github.com/apostoldevel/db-platform).

Key database objects:

| Object | Purpose |
|--------|---------|
| `replication.outbox` | Slot-fed outbox with priority column |
| `replication.peer` | Per-peer watermarks (sent_id, received_id) |
| `replication.sync_log` | Audit journal (direction, channel, entries, bytes) |
| `replication.list` | Table registry with priority settings |
| `replication.drain(limit)` | Read slot → parse wal2json → INSERT into outbox |
| `replication.fetch_outbox(peer, channel, limit)` | Batch for peer filtered by priority |
| `replication.apply_batch(source, entries)` | Atomic apply with DEFERRED constraints |
| `replication.ack(source, received_max_id)` | Update peer watermarks |

**Fallback**: the process can use existing db-platform functions (`api.replication_log`, `api.add_to_relay_log`, `api.replication_apply`) before the new functions are available.

Configuration
-

In the application config (`conf/apostol.json`):

```json
{
  "module": {
    "Replication": {
      "enable": true,
      "mode": "automatic",
      "channel": "lan",
      "master": "https://master.example.com",
      "source": "vessel-aurora",
      "interval": {
        "lan": 30,
        "wifi": 60,
        "satellite": 300
      },
      "batch_limit": 500,
      "drain_limit": 1000,
      "oauth2": "replication.json"
    }
  }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable` | bool | `false` | Enable/disable the process |
| `mode` | string | `automatic` | Sync mode: `automatic`, `paused`, `manual` |
| `channel` | string | `lan` | Active channel: `lan`, `wifi`, `satellite` |
| `master` | string | — | Master node URL |
| `source` | string | hostname | This node's identifier |
| `interval` | object | `{30,60,300}` | Sync interval per channel (seconds) |
| `batch_limit` | int | `500` | Max entries per sync batch |
| `drain_limit` | int | `1000` | Max entries to drain from slot per heartbeat |
| `oauth2` | string | — | Path to OAuth2 credentials JSON file |

The process also requires:
* `postgres.helper` connection string in the config
* OAuth2 credentials file with `client_id`, `client_secret`, `token_uri`

Build requirements: `WITH_POSTGRESQL`.

Installation
-

Follow the build and installation instructions for [Апостол (C++20)](https://github.com/apostoldevel/libapostol#build-and-installation).

[^crm]: **Apostol CRM** — шаблон-проект построенный на фреймворках [A-POST-OL](https://github.com/apostoldevel/libapostol) (C++20) и [PostgreSQL Framework for Backend Development](https://github.com/apostoldevel/db-platform).
