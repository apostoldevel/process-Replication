#pragma once

#ifdef WITH_POSTGRESQL

#include "apostol/process_module.hpp"
#include "apostol/bot_session.hpp"
#include "apostol/pg.hpp"
#include "apostol/fetch_client.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace apostol
{

class Application;
class EventLoop;
class Logger;

// --- ReplicationServer -------------------------------------------------------
//
// Background process module that synchronizes data between Apostol CRM nodes.
//
// Designed for low-bandwidth channels (satellite): stateless HTTP REST transport,
// gzip compression, priority-based filtering, offline accumulation.
//
// Architecture: ProcessModule injected into generic ModuleProcess shell.
//
// Data flow:
//   [WAL] -> [logical replication slot] -> [replication.outbox] -> HTTP sync
//
// Heartbeat cycle:
//   1. refresh_token()         -- remote OAuth2 via FetchClient
//   2. drain_slot()            -- always (even when paused): slot -> outbox
//   3. process_notify_queue()  -- LISTEN "replication_cmd" (mode/channel/sync)
//   4. start_sync()            -- if auto mode and interval elapsed
//
// Sync cycle (single HTTP round-trip):
//   1. Collect outgoing batch:   replication.fetch_outbox(peer, channel, limit)
//   2. POST master/replication/sync: {source, max_id, entries}
//   3. Apply incoming batch:     replication.apply_batch(source, entries)
//   4. Update watermarks:        replication.ack(source, ids)
//
// Fallback: uses existing db-platform API functions when new ones are unavailable.
//
// Configuration (in apostol.json):
//   "module": {
//     "Replication": {
//       "enable": true,
//       "mode": "automatic",
//       "channel": "lan",
//       "master": "https://master.example.com",
//       "source": "vessel-aurora",
//       "interval": { "lan": 30, "wifi": 60, "satellite": 300 },
//       "batch_limit": 500,
//       "drain_limit": 1000,
//       "oauth2": "replication.json"
//     }
//   }
//
class ReplicationServer final : public ProcessModule
{
public:
    std::string_view name() const override { return "replication-server"; }

    void on_start(EventLoop& loop, Application& app) override;
    void heartbeat(std::chrono::system_clock::time_point now) override;
    void on_stop() override;

private:
    using time_point = std::chrono::system_clock::time_point;
    using seconds    = std::chrono::seconds;

    // -- Enums ----------------------------------------------------------------

    enum class SyncMode { automatic, paused, manual };
    enum class Channel  { lan, wifi, satellite };
    enum class Status   { stopped, authenticating, running };

    // -- State ----------------------------------------------------------------

    PgPool*     pool_{nullptr};
    Logger*     logger_{nullptr};
    EventLoop*  loop_{nullptr};

    std::unique_ptr<BotSession>  bot_;    // local DB auth (apibot)
    std::unique_ptr<FetchClient> fetch_;

    Status   status_{Status::stopped};
    SyncMode mode_{SyncMode::automatic};
    Channel  channel_{Channel::lan};

    // Remote OAuth2
    std::string master_url_;
    std::string source_;         // this node's ID
    std::string access_token_;
    time_point  token_expires_{};
    std::string oauth2_file_;    // path to credentials JSON
    std::string client_id_;      // cached from oauth2 file
    std::string client_secret_;
    std::string token_uri_{"/oauth2/token"};

    // Sync state
    bool         sync_in_progress_{false};
    time_point   next_sync_{};
    time_point   next_token_retry_{};  // backoff for remote OAuth2 retries
    time_point   last_sync_{};
    std::string  last_error_;
    std::size_t  sync_count_{0};
    std::size_t  error_count_{0};
    std::size_t  consecutive_errors_{0};
    std::size_t  batch_limit_{500};
    std::size_t  drain_limit_{1000};

    // Interval per channel (seconds)
    seconds interval_lan_{30};
    seconds interval_wifi_{60};
    seconds interval_satellite_{300};

    // NOTIFY queue (from "replication_cmd")
    std::vector<std::string> pending_commands_;
    std::size_t  max_pending_commands_{100};

    // -- Config ---------------------------------------------------------------
    void load_config(Application& app);
    seconds current_interval() const;
    int max_priority() const;
    static SyncMode parse_mode(std::string_view s);
    static Channel  parse_channel(std::string_view s);

    // -- Remote OAuth2 --------------------------------------------------------
    void refresh_token();
    void on_token_response(FetchResponse resp);
    void on_token_error(std::string_view error);
    bool token_valid() const;

    // -- Drain slot -> outbox -------------------------------------------------
    void drain_slot();

    // -- NOTIFY ---------------------------------------------------------------
    void on_notify(std::string_view payload);
    void process_notify_queue();
    void handle_command(const std::string& cmd);

    // -- Sync -----------------------------------------------------------------
    void start_sync();
    void on_outbox_ready(std::vector<PgResult> results);
    void on_sync_response(FetchResponse resp);
    void on_apply_done(std::vector<PgResult> results);
    void on_sync_error(const std::string& error);

    void schedule_next_sync(bool has_more = false);

    // -- Logging --------------------------------------------------------------
    void on_fatal(const std::string& error);
};

} // namespace apostol

#endif // WITH_POSTGRESQL
