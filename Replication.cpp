#ifdef WITH_POSTGRESQL

#include "Replication/Replication.hpp"

#include "apostol/application.hpp"
#include "apostol/pg_utils.hpp"

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <fstream>
#include <unistd.h>

namespace apostol
{

// --- helpers -----------------------------------------------------------------

ReplicationServer::SyncMode ReplicationServer::parse_mode(std::string_view s)
{
    if (s == "paused")  return SyncMode::paused;
    if (s == "manual")  return SyncMode::manual;
    return SyncMode::automatic;
}

ReplicationServer::Channel ReplicationServer::parse_channel(std::string_view s)
{
    if (s == "wifi")      return Channel::wifi;
    if (s == "satellite") return Channel::satellite;
    return Channel::lan;
}

ReplicationServer::seconds ReplicationServer::current_interval() const
{
    switch (channel_) {
        case Channel::wifi:      return interval_wifi_;
        case Channel::satellite: return interval_satellite_;
        default:                 return interval_lan_;
    }
}

int ReplicationServer::max_priority() const
{
    switch (channel_) {
        case Channel::satellite: return 1;  // high only
        case Channel::wifi:      return 2;  // high + medium
        default:                 return 3;  // all
    }
}

bool ReplicationServer::token_valid() const
{
    return !access_token_.empty()
        && std::chrono::system_clock::now() < token_expires_;
}

// --- load_config -------------------------------------------------------------

void ReplicationServer::load_config(Application& app)
{
    auto* cfg = app.module_config("Replication");
    if (!cfg)
        return;

    auto& c = *cfg;

    if (c.contains("mode") && c["mode"].is_string())
        mode_ = parse_mode(c["mode"].get<std::string>());

    if (c.contains("channel") && c["channel"].is_string())
        channel_ = parse_channel(c["channel"].get<std::string>());

    if (c.contains("master") && c["master"].is_string())
        master_url_ = c["master"].get<std::string>();

    if (c.contains("source") && c["source"].is_string())
        source_ = c["source"].get<std::string>();

    if (c.contains("batch_limit") && c["batch_limit"].is_number_unsigned())
        batch_limit_ = c["batch_limit"].get<std::size_t>();

    if (c.contains("drain_limit") && c["drain_limit"].is_number_unsigned())
        drain_limit_ = c["drain_limit"].get<std::size_t>();

    if (c.contains("oauth2") && c["oauth2"].is_string()) {
        oauth2_file_ = c["oauth2"].get<std::string>();

        // Cache credentials from oauth2 file (read once, not every heartbeat)
        std::ifstream f(oauth2_file_);
        if (f.is_open()) {
            try {
                nlohmann::json creds;
                f >> creds;
                client_id_     = creds.value("client_id", "");
                client_secret_ = creds.value("client_secret", "");
                token_uri_     = creds.value("token_uri", "/oauth2/token");
            } catch (...) {
                // Will be logged on first refresh_token() attempt
            }
        }
    }

    // Interval per channel
    if (c.contains("interval") && c["interval"].is_object()) {
        auto& iv = c["interval"];
        if (iv.contains("lan") && iv["lan"].is_number())
            interval_lan_ = seconds(iv["lan"].get<int>());
        if (iv.contains("wifi") && iv["wifi"].is_number())
            interval_wifi_ = seconds(iv["wifi"].get<int>());
        if (iv.contains("satellite") && iv["satellite"].is_number())
            interval_satellite_ = seconds(iv["satellite"].get<int>());
    }
}

// --- on_start ----------------------------------------------------------------

void ReplicationServer::on_start(EventLoop& loop, Application& app)
{
    pool_   = &app.db_pool();
    logger_ = &app.logger();
    loop_   = &loop;

    // FetchClient for HTTP sync + OAuth2 token fetch
    fetch_ = std::make_unique<FetchClient>(loop);
    fetch_->set_timeout(30000);  // 30s — important for satellite channels

    // Load config first (to populate source_ and cache oauth2 credentials)
    load_config(app);

    // Determine source name (defaults to hostname)
    if (source_.empty()) {
        char hostname[256]{};
        ::gethostname(hostname, sizeof(hostname));
        source_ = hostname;
    }

    // LISTEN for operator commands (mode/channel/sync)
    pool_->listen("replication_cmd",
        [this](std::string_view /*channel*/, std::string_view payload) {
            on_notify(payload);
        });

    logger_->notice("ReplicationServer started (source={}, master={}, mode={}, channel={})",
                    source_, master_url_,
                    mode_ == SyncMode::automatic ? "automatic" :
                        mode_ == SyncMode::paused ? "paused" : "manual",
                    channel_ == Channel::lan ? "lan" :
                        channel_ == Channel::wifi ? "wifi" : "satellite");
}

// --- heartbeat ---------------------------------------------------------------

void ReplicationServer::heartbeat(std::chrono::system_clock::time_point now)
{
    if (!fetch_ || !pool_)
        return;

    // 1. Refresh remote OAuth2 token
    if (!token_valid() && status_ != Status::authenticating)
        refresh_token();

    if (!token_valid())
        return;

    if (status_ == Status::stopped)
        status_ = Status::running;

    // 2. Drain logical replication slot -> outbox (always, even when paused)
    drain_slot();

    // 3. Process NOTIFY commands
    process_notify_queue();

    // 4. Sync if auto and interval elapsed
    if (sync_in_progress_)
        return;

    if (mode_ == SyncMode::automatic && now >= next_sync_)
        start_sync();
}

// --- on_stop -----------------------------------------------------------------

void ReplicationServer::on_stop()
{
    if (pool_)
        pool_->unlisten("replication_cmd");
    fetch_.reset();
    access_token_.clear();
}

// --- Remote OAuth2 -----------------------------------------------------------

void ReplicationServer::refresh_token()
{
    if (master_url_.empty() || client_id_.empty() || client_secret_.empty())
        return;

    status_ = Status::authenticating;

    std::string url = master_url_ + token_uri_;
    std::string body = fmt::format(
        "grant_type=client_credentials&client_id={}&client_secret={}",
        client_id_, client_secret_);

    std::vector<std::pair<std::string, std::string>> headers = {
        {"Content-Type", "application/x-www-form-urlencoded"}
    };

    fetch_->post(url, body, headers,
        [this](FetchResponse resp) { on_token_response(std::move(resp)); },
        [this](std::string_view err) { on_token_error(err); });
}

void ReplicationServer::on_token_response(FetchResponse resp)
{
    if (resp.status_code < 200 || resp.status_code >= 300) {
        on_token_error(fmt::format("HTTP {}: {}", resp.status_code,
                                   resp.body.substr(0, 256)));
        return;
    }

    try {
        auto j = nlohmann::json::parse(resp.body);
        access_token_ = j.value("access_token", "");
        int expires_in = j.value("expires_in", 3600);
        // Refresh early: 5 min or half of expires_in (whichever is smaller)
        int margin = std::min(300, expires_in / 2);
        token_expires_ = std::chrono::system_clock::now()
                       + std::chrono::seconds(expires_in - margin);
        status_ = Status::running;
        consecutive_errors_ = 0;
        logger_->notice("ReplicationServer: authenticated with {}", master_url_);
    } catch (const std::exception& e) {
        on_token_error(e.what());
    }
}

void ReplicationServer::on_token_error(std::string_view error)
{
    logger_->error("ReplicationServer: OAuth2 failed: {}", error);
    status_ = Status::stopped;
    access_token_.clear();
    // Retry after backoff
    ++consecutive_errors_;
    auto backoff = std::min(seconds(10 * (1 << std::min(consecutive_errors_, std::size_t(8)))),
                            seconds(1800));
    next_sync_ = std::chrono::system_clock::now() + backoff;
}

// --- Drain slot -> outbox ----------------------------------------------------

void ReplicationServer::drain_slot()
{
    // Use existing replication.log as fallback when logical decoding
    // functions are not yet available in db-platform.
    // When replication.drain() is available, switch to:
    //   SELECT replication.drain(<drain_limit_>)
    //
    // For now, drain is a no-op -- outbox IS replication.log
    // (populated by existing triggers in db-platform).
}

// --- NOTIFY ------------------------------------------------------------------

void ReplicationServer::on_notify(std::string_view payload)
{
    pending_commands_.emplace_back(payload);
}

void ReplicationServer::process_notify_queue()
{
    if (pending_commands_.empty())
        return;

    auto cmds = std::move(pending_commands_);
    pending_commands_.clear();

    for (auto& cmd : cmds)
        handle_command(cmd);
}

void ReplicationServer::handle_command(const std::string& cmd)
{
    try {
        auto j = nlohmann::json::parse(cmd);
        std::string action = j.value("action", "");

        if (action == "sync") {
            // Immediate sync request
            if (!sync_in_progress_ && token_valid())
                start_sync();
        } else if (action == "mode") {
            auto old = mode_;
            mode_ = parse_mode(j.value("mode", "automatic"));
            if (mode_ != old) {
                logger_->notice("ReplicationServer: mode changed to {}",
                    mode_ == SyncMode::automatic ? "automatic" :
                        mode_ == SyncMode::paused ? "paused" : "manual");
                if (mode_ == SyncMode::automatic)
                    next_sync_ = std::chrono::system_clock::now();
            }
        } else if (action == "channel") {
            auto old = channel_;
            channel_ = parse_channel(j.value("channel", "lan"));
            if (channel_ != old) {
                logger_->notice("ReplicationServer: channel changed to {}",
                    channel_ == Channel::lan ? "lan" :
                        channel_ == Channel::wifi ? "wifi" : "satellite");
            }
        }
    } catch (const std::exception& e) {
        logger_->warn("ReplicationServer: invalid command: {}", e.what());
    }
}

// --- Sync --------------------------------------------------------------------

void ReplicationServer::start_sync()
{
    if (master_url_.empty() || source_.empty()) {
        logger_->warn("ReplicationServer: master_url or source not configured");
        return;
    }

    sync_in_progress_ = true;

    // Step 1: Collect outgoing batch from local DB
    //
    // Fallback: uses existing api.replication_log(from, source, limit).
    // When replication.fetch_outbox(peer, channel, limit) is available,
    // switch to that for priority-based filtering.
    //
    auto sql = fmt::format(
        "SELECT * FROM api.replication_log(0, {}, {})",
        pq_quote_literal(source_),
        batch_limit_);

    pool_->execute(sql,
        [this](std::vector<PgResult> results) {
            on_outbox_ready(std::move(results));
        },
        [this](std::string_view error) {
            on_sync_error(std::string(error));
        });
}

void ReplicationServer::on_outbox_ready(std::vector<PgResult> results)
{
    if (results.empty() || !results[0].ok()) {
        on_sync_error("Failed to read outbox");
        return;
    }

    auto& res = results[0];

    // Build JSON payload
    nlohmann::json payload;
    payload["source"] = source_;
    payload["entries"] = nlohmann::json::array();

    int rows = res.rows();
    for (int r = 0; r < rows; ++r) {
        nlohmann::json entry;
        for (int c = 0; c < res.columns(); ++c) {
            const char* col_name = res.column_name(c);
            const char* val = res.value(r, c);
            if (col_name && val)
                entry[col_name] = val;
        }
        payload["entries"].push_back(std::move(entry));
    }

    // Step 2: POST to master
    std::string body = payload.dump();
    std::string url = master_url_ + "/api/v1/replication/sync";

    std::vector<std::pair<std::string, std::string>> headers = {
        {"Authorization", "Bearer " + access_token_},
        {"Content-Type",  "application/json"},
        {"Accept-Encoding", "gzip"}
    };

    fetch_->post(url, body, headers,
        [this](FetchResponse resp) { on_sync_response(std::move(resp)); },
        [this](std::string_view err) {
            on_sync_error(std::string(err));
        });
}

void ReplicationServer::on_sync_response(FetchResponse resp)
{
    if (resp.status_code < 200 || resp.status_code >= 300) {
        on_sync_error(fmt::format("HTTP {}: {}", resp.status_code,
                                  resp.body.substr(0, 256)));
        return;
    }

    // Step 3: Parse response and apply incoming batch
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(resp.body);
    } catch (const std::exception& e) {
        on_sync_error(fmt::format("Cannot parse sync response: {}", e.what()));
        return;
    }

    auto entries = j.value("entries", nlohmann::json::array());
    bool has_more = j.value("has_more", false);

    if (entries.empty()) {
        // Nothing to apply -- sync done
        last_sync_ = std::chrono::system_clock::now();
        last_error_.clear();
        ++sync_count_;
        consecutive_errors_ = 0;
        sync_in_progress_ = false;
        schedule_next_sync(has_more);

        logger_->notice("ReplicationServer: sync completed, no incoming entries");
        return;
    }

    // Apply incoming batch using existing api.add_to_relay_log + api.replication_apply
    // When replication.apply_batch() is available, switch to that.
    std::string sql;
    for (auto& entry : entries) {
        sql += fmt::format(
            "SELECT * FROM api.add_to_relay_log({}, {}, {}::timestamptz, "
            "{}::char, {}, {}, {}::jsonb, {}::jsonb, false);\n",
            pq_quote_literal(entry.value("source", source_)),
            entry.value("id", 0),
            pq_quote_literal(entry.value("datetime", "")),
            pq_quote_literal(entry.value("action", "")),
            pq_quote_literal(entry.value("schema", "")),
            pq_quote_literal(entry.value("name", "")),
            pq_quote_literal(entry.value("key", "null")),
            pq_quote_literal(entry.value("data", "null")));
    }

    sql += fmt::format("SELECT * FROM api.replication_apply({});\n",
                       pq_quote_literal(source_));

    pool_->execute(sql,
        [this, has_more](std::vector<PgResult> results) {
            on_apply_done(std::move(results));
            schedule_next_sync(has_more);
        },
        [this](std::string_view error) {
            on_sync_error(std::string(error));
        });
}

void ReplicationServer::on_apply_done(std::vector<PgResult> results)
{
    last_sync_ = std::chrono::system_clock::now();
    last_error_.clear();
    ++sync_count_;
    consecutive_errors_ = 0;
    sync_in_progress_ = false;

    // Count applied entries (last result is replication_apply)
    int applied = 0;
    if (!results.empty() && results.back().ok() && results.back().rows() > 0) {
        const char* val = results.back().value(0, 0);
        if (val) applied = std::atoi(val);
    }

    logger_->notice("ReplicationServer: sync completed, applied {} entries", applied);
}

void ReplicationServer::on_sync_error(const std::string& error)
{
    sync_in_progress_ = false;
    last_error_ = error;
    ++error_count_;
    ++consecutive_errors_;

    logger_->error("ReplicationServer: sync error: {}", error);

    // Backoff: double interval, max 30 minutes
    auto backoff = std::min(
        current_interval() * (1 << std::min(consecutive_errors_, std::size_t(4))),
        seconds(1800));
    next_sync_ = std::chrono::system_clock::now() + backoff;
}

void ReplicationServer::schedule_next_sync(bool has_more)
{
    if (has_more) {
        // Fast drain: more data available, sync again in 5 seconds
        next_sync_ = std::chrono::system_clock::now() + seconds(5);
    } else {
        next_sync_ = std::chrono::system_clock::now() + current_interval();
    }
}

// --- on_fatal ----------------------------------------------------------------

void ReplicationServer::on_fatal(const std::string& error)
{
    status_ = Status::stopped;
    next_sync_ = std::chrono::system_clock::now() + seconds(30);
    logger_->error("ReplicationServer: fatal error, pausing 30s: {}", error);
}

} // namespace apostol

#endif // WITH_POSTGRESQL
