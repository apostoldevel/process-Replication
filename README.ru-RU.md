[![en](https://img.shields.io/badge/lang-en-green.svg)](README.md)

Сервер репликации
-

**Процесс** для [Apostol](https://github.com/apostoldevel/apostol) + [db-platform](https://github.com/apostoldevel/db-platform) — **Apostol CRM**[^crm].

Описание
-

**Сервер репликации** — фоновый процесс-модуль для фреймворка [Апостол](https://github.com/apostoldevel/apostol). Синхронизирует данные между нодами Apostol CRM по HTTP REST со сжатием gzip. Спроектирован для каналов с низкой пропускной способностью (спутниковая связь на морских судах).

Основные характеристики:

* Написан на C++20 с использованием асинхронной неблокирующей модели ввода-вывода на базе **epoll** API.
* Подключается к **PostgreSQL** через `libpq`, используя роль `apibot` (пул соединений helper).
* Аутентифицируется на удалённом мастере через **OAuth2 `client_credentials`** с помощью `FetchClient`.
* **NOTIFY-driven**: подписывается на PostgreSQL-канал `LISTEN replication_cmd` для операторских команд (режим/канал/синхронизация).
* **Автоматическая синхронизация**: по таймеру с настраиваемым интервалом для каждого типа канала.
* **Фильтрация по приоритету**: спутник отправляет только данные высокого приоритета, LAN отправляет всё.
* **Накопление при отсутствии связи**: данные всегда захватываются и хранятся локально, синхронизируются при появлении связи.
* **Один HTTP round-trip за цикл синхронизации**: slave отправляет свой пакет и получает пакет мастера в одном POST/ответе.

### Архитектура

Сервер репликации следует паттерну **ProcessModule**, введённому в apostol.v2:

```
Application
  └── ModuleProcess (generic-оболочка процесса: сигналы, EventLoop, PgPool)
        └── ReplicationServer (ProcessModule: только бизнес-логика)
```

Жизненный цикл процесса (обработка сигналов, crash recovery, настройка PgPool, таймер heartbeat) управляется generic-оболочкой `ModuleProcess`. `ReplicationServer` содержит только логику синхронизации.

### Поток данных

```
[Приложение] → INSERT/UPDATE/DELETE → [PostgreSQL WAL]
                                            │
[Publication: apostol_repl]                 │
[Slot: apostol_repl]  ◄────────────────────┘
      │
[ReplicationServer.drain_slot()]
      │  pg_logical_slot_get_changes() → wal2json
      ▼
[replication.outbox] ← приоритет из replication.list
      │
[sync()] ── FetchClient POST → remote /replication/sync
      │                            │
      │     ◄── ответ ─────────────┘
      ▼
[apply_batch()] → INSERT/UPDATE/DELETE (DEFERRED constraints)
```

### Цикл синхронизации

```
heartbeat (1 сек)
  └── refresh_token()            — удалённый OAuth2 через FetchClient
  └── drain_slot()               — выполняется всегда (даже в режиме паузы)
  └── process_notify_queue()     — LISTEN "replication_cmd"
  └── если automatic && интервал истёк:
        └── start_sync()
              1. fetch_outbox(peer, channel, limit)  → собрать локальный пакет
              2. POST master/replication/sync         → отправить пакет, получить ответный
              3. apply_batch(source, entries)          → применить входящие записи
              4. ack(source, max_id)                  → обновить watermarks
              5. schedule_next_sync()                 → 5с если has_more, иначе интервал
```

### Режимы

| Режим | Описание |
|-------|----------|
| `automatic` | Синхронизация по интервалу heartbeat (по умолчанию) |
| `paused` | Данные накапливаются в outbox, но не отправляются |
| `manual` | Синхронизация только по явной команде через NOTIFY |

Режимы можно переключать в рантайме через `NOTIFY replication_cmd '{"action":"mode","mode":"paused"}'`.

### Каналы

| Канал | Интервал | Фильтр приоритета | Отправляемые таблицы |
|-------|----------|-------------------|---------------------|
| `lan` | 30с | все (1-3) | Все данные |
| `wifi` | 60с | высокий + средний (1-2) | Большинство таблиц |
| `satellite` | 300с | только высокий (1) | Только критические данные |

Все изменения всегда сохраняются в outbox. При переключении канала со спутника на LAN накопленные записи среднего и низкого приоритета будут отправлены.

Модуль базы данных
-

ReplicationServer связан с модулем **`replication`** платформы [db-platform](https://github.com/apostoldevel/db-platform).

Ключевые объекты базы данных:

| Объект | Назначение |
|--------|-----------|
| `replication.outbox` | Outbox из slot с колонкой приоритета |
| `replication.peer` | Watermarks по пирам (sent_id, received_id) |
| `replication.sync_log` | Журнал аудита (направление, канал, записи, байты) |
| `replication.list` | Реестр таблиц с настройками приоритета |
| `replication.drain(limit)` | Чтение slot → парсинг wal2json → INSERT в outbox |
| `replication.fetch_outbox(peer, channel, limit)` | Пакет для пира с фильтрацией по приоритету |
| `replication.apply_batch(source, entries)` | Атомарное применение с DEFERRED constraints |
| `replication.ack(source, received_max_id)` | Обновление watermarks пира |

**Fallback**: процесс может использовать существующие функции db-platform (`api.replication_log`, `api.add_to_relay_log`, `api.replication_apply`) до появления новых функций.

Конфигурация
-

В конфигурационном файле приложения (`conf/apostol.json`):

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

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|-------------|----------|
| `enable` | bool | `false` | Включить/отключить процесс |
| `mode` | string | `automatic` | Режим синхронизации: `automatic`, `paused`, `manual` |
| `channel` | string | `lan` | Активный канал: `lan`, `wifi`, `satellite` |
| `master` | string | — | URL мастер-ноды |
| `source` | string | hostname | Идентификатор этой ноды |
| `interval` | object | `{30,60,300}` | Интервал синхронизации по каналу (секунды) |
| `batch_limit` | int | `500` | Макс. записей на один пакет синхронизации |
| `drain_limit` | int | `1000` | Макс. записей для drain из slot за heartbeat |
| `oauth2` | string | — | Путь к JSON-файлу с OAuth2 credentials |

Также необходимы:
* Строка подключения `postgres.helper` в конфигурации
* Файл OAuth2 credentials с `client_id`, `client_secret`, `token_uri`

Требования к сборке: `WITH_POSTGRESQL`.

Установка
-

Следуйте указаниям по сборке и установке [Апостол](https://github.com/apostoldevel/apostol#building-and-installation).

[^crm]: **Apostol CRM** — абстрактный термин, а не самостоятельный продукт. Он обозначает любой проект, в котором совместно используются фреймворк [Apostol](https://github.com/apostoldevel/apostol) (C++) и [db-platform](https://github.com/apostoldevel/db-platform) через специально разработанные модули и процессы. Каждый фреймворк можно использовать независимо; вместе они образуют полноценную backend-платформу.
