import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path

from src.config import Config


class Database:
    """SQLite database for bot state, logs, and sent items tracking."""

    def __init__(self, db_path: Path | None = None):
        self.db_path = db_path or Config.DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_tables()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_tables(self) -> None:
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS bot_state (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS activity_log (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp  TEXT    NOT NULL,
                    source     TEXT    NOT NULL,
                    action     TEXT    NOT NULL,
                    summary    TEXT,
                    status     TEXT    NOT NULL DEFAULT 'ok',
                    details    TEXT
                );

                CREATE TABLE IF NOT EXISTS sent_items (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    source     TEXT    NOT NULL,
                    item_hash  TEXT    NOT NULL,
                    sent_at    TEXT    NOT NULL,
                    UNIQUE(source, item_hash)
                );

                CREATE TABLE IF NOT EXISTS source_status (
                    source        TEXT PRIMARY KEY,
                    last_check    TEXT,
                    last_success  TEXT,
                    last_error    TEXT,
                    last_error_msg TEXT NOT NULL DEFAULT '',
                    fail_count    INTEGER NOT NULL DEFAULT 0,
                    checks_today  INTEGER NOT NULL DEFAULT 0,
                    errors_today  INTEGER NOT NULL DEFAULT 0,
                    enabled       INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS token_usage (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp       TEXT    NOT NULL,
                    source          TEXT    NOT NULL,
                    input_tokens    INTEGER NOT NULL,
                    output_tokens   INTEGER NOT NULL,
                    cost_usd        REAL    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS message_queue (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp           TEXT    NOT NULL,
                    source              TEXT    NOT NULL,
                    importance          INTEGER NOT NULL DEFAULT 3,
                    category            TEXT    NOT NULL DEFAULT '',
                    region              TEXT    NOT NULL DEFAULT '',
                    title               TEXT    NOT NULL DEFAULT '',
                    summary             TEXT    NOT NULL DEFAULT '',
                    impact              TEXT    NOT NULL DEFAULT '',
                    sent_to_channel     INTEGER NOT NULL DEFAULT 0,
                    included_in_digest  INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS ff_events (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_hash          TEXT    UNIQUE NOT NULL,
                    title               TEXT    NOT NULL,
                    country             TEXT    NOT NULL DEFAULT '',
                    event_time_utc      TEXT    NOT NULL DEFAULT '',
                    event_time_london   TEXT    NOT NULL DEFAULT '',
                    forecast            TEXT    NOT NULL DEFAULT '',
                    previous            TEXT    NOT NULL DEFAULT '',
                    reminder_sent       INTEGER NOT NULL DEFAULT 0,
                    created_at          TEXT    NOT NULL
                );

                -- Initialize bot_paused state if not exists
                INSERT OR IGNORE INTO bot_state (key, value)
                VALUES ('paused', 'false');
            """)

    # --- Bot State ---

    def is_paused(self) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value FROM bot_state WHERE key = 'paused'"
            ).fetchone()
            return row["value"] == "true" if row else False

    def set_paused(self, paused: bool) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE bot_state SET value = ? WHERE key = 'paused'",
                ("true" if paused else "false",),
            )

    def get_importance_threshold(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value FROM bot_state WHERE key = 'importance_threshold'"
            ).fetchone()
            return int(row["value"]) if row else 1

    def set_importance_threshold(self, threshold: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO bot_state (key, value) VALUES ('importance_threshold', ?)",
                (str(threshold),),
            )

    # --- Activity Logging ---

    def log_activity(
        self,
        source: str,
        action: str,
        summary: str | None = None,
        status: str = "ok",
        details: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO activity_log
                   (timestamp, source, action, summary, status, details)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    datetime.now(timezone.utc).isoformat(),
                    source,
                    action,
                    summary,
                    status,
                    details,
                ),
            )

    def get_recent_logs(self, limit: int = 10) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT timestamp, source, action, summary, status
                   FROM activity_log
                   ORDER BY id DESC LIMIT ?""",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    # --- Duplicate Detection ---

    def is_already_sent(self, source: str, item_hash: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM sent_items WHERE source = ? AND item_hash = ?",
                (source, item_hash),
            ).fetchone()
            return row is not None

    def mark_as_sent(self, source: str, item_hash: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO sent_items (source, item_hash, sent_at)
                   VALUES (?, ?, ?)""",
                (source, item_hash, datetime.now(timezone.utc).isoformat()),
            )

    # --- Source Status Tracking ---

    def update_source_status(
        self, source: str, success: bool, error_msg: str = ""
    ) -> dict:
        """Update source status. Returns dict with fail_count and was_failing flag."""
        now = datetime.now(timezone.utc).isoformat()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT fail_count, last_success, last_check, checks_today, errors_today FROM source_status WHERE source = ?",
                (source,),
            ).fetchone()

            was_failing = False

            if existing is None:
                fail_count = 0 if success else 1
                conn.execute(
                    """INSERT INTO source_status
                       (source, last_check, last_success, last_error, last_error_msg,
                        fail_count, checks_today, errors_today)
                       VALUES (?, ?, ?, ?, ?, ?, 1, ?)""",
                    (source, now, now if success else None,
                     None if success else now, error_msg,
                     fail_count, 0 if success else 1),
                )
            elif success:
                was_failing = existing["fail_count"] > 0
                fail_count = 0
                # Reset daily counters if new day
                checks = existing["checks_today"] + 1
                errors = existing["errors_today"]
                if existing["last_check"] and not existing["last_check"].startswith(today):
                    checks = 1
                    errors = 0
                conn.execute(
                    """UPDATE source_status
                       SET last_check = ?, last_success = ?, fail_count = 0,
                           checks_today = ?, errors_today = ?
                       WHERE source = ?""",
                    (now, now, checks, errors, source),
                )
            else:
                was_failing = existing["fail_count"] > 0
                fail_count = existing["fail_count"] + 1
                checks = existing["checks_today"] + 1
                errors = existing["errors_today"] + 1
                if existing["last_check"] and not existing["last_check"].startswith(today):
                    checks = 1
                    errors = 1
                conn.execute(
                    """UPDATE source_status
                       SET last_check = ?, last_error = ?, last_error_msg = ?,
                           fail_count = ?, checks_today = ?, errors_today = ?
                       WHERE source = ?""",
                    (now, now, error_msg[:200], fail_count, checks, errors, source),
                )

            return {"fail_count": fail_count, "was_failing": was_failing}

    def get_all_source_statuses(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT source, last_check, last_success, last_error,
                          last_error_msg, fail_count, checks_today,
                          errors_today, enabled
                   FROM source_status ORDER BY source"""
            ).fetchall()
            return [dict(r) for r in rows]

    # --- Token Usage Tracking ---

    def log_token_usage(
        self,
        source: str,
        input_tokens: int,
        output_tokens: int,
        cost_usd: float,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO token_usage
                   (timestamp, source, input_tokens, output_tokens, cost_usd)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    datetime.now(timezone.utc).isoformat(),
                    source,
                    input_tokens,
                    output_tokens,
                    cost_usd,
                ),
            )

    def get_today_cost(self) -> float:
        """Get total AI cost spent today in USD."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._connect() as conn:
            row = conn.execute(
                """SELECT COALESCE(SUM(cost_usd), 0) as total
                   FROM token_usage
                   WHERE timestamp LIKE ?""",
                (f"{today}%",),
            ).fetchone()
            return row["total"]

    def get_today_tokens(self) -> dict:
        """Get today's token usage summary."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._connect() as conn:
            row = conn.execute(
                """SELECT
                       COALESCE(SUM(input_tokens), 0)  as input_total,
                       COALESCE(SUM(output_tokens), 0) as output_total,
                       COALESCE(SUM(cost_usd), 0)      as cost_total,
                       COUNT(*)                         as call_count
                   FROM token_usage
                   WHERE timestamp LIKE ?""",
                (f"{today}%",),
            ).fetchone()
            return dict(row)

    def get_spending_by_source_today(self) -> list[dict]:
        """Get today's spending broken down by source."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT source,
                       COUNT(*) as calls,
                       COALESCE(SUM(cost_usd), 0) as cost
                   FROM token_usage
                   WHERE timestamp LIKE ?
                   GROUP BY source
                   ORDER BY cost DESC""",
                (f"{today}%",),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_spending_daily_history(self, days: int = 7) -> list[dict]:
        """Get daily spending totals for the last N days."""
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT
                       SUBSTR(timestamp, 1, 10) as date,
                       COUNT(*) as calls,
                       COALESCE(SUM(cost_usd), 0) as cost
                   FROM token_usage
                   GROUP BY SUBSTR(timestamp, 1, 10)
                   ORDER BY date DESC
                   LIMIT ?""",
                (days,),
            ).fetchall()
            return [dict(r) for r in rows]

    # --- Message Queue ---

    def add_to_queue(
        self,
        source: str,
        importance: int,
        category: str,
        region: str,
        title: str,
        summary: str,
        impact: str,
    ) -> int:
        """Add an analyzed item to the message queue. Returns the row ID."""
        with self._connect() as conn:
            cursor = conn.execute(
                """INSERT INTO message_queue
                   (timestamp, source, importance, category, region,
                    title, summary, impact)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    datetime.now(timezone.utc).isoformat(),
                    source,
                    importance,
                    category,
                    region,
                    title,
                    summary,
                    impact,
                ),
            )
            return cursor.lastrowid

    def mark_queue_sent(self, queue_id: int) -> None:
        """Mark a queue item as sent to the channel."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE message_queue SET sent_to_channel = 1 WHERE id = ?",
                (queue_id,),
            )

    def mark_queue_digested(self, queue_ids: list[int]) -> None:
        """Mark multiple queue items as included in a digest."""
        if not queue_ids:
            return
        with self._connect() as conn:
            placeholders = ",".join("?" for _ in queue_ids)
            conn.execute(
                f"UPDATE message_queue SET included_in_digest = 1 WHERE id IN ({placeholders})",
                queue_ids,
            )

    def get_unsent_queue_items(self) -> list[dict]:
        """Get all items that haven't been sent or included in a digest."""
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT id, timestamp, source, importance, category,
                          region, title, summary, impact
                   FROM message_queue
                   WHERE sent_to_channel = 0 AND included_in_digest = 0
                   ORDER BY timestamp ASC"""
            ).fetchall()
            return [dict(r) for r in rows]

    def get_pending_digest_items(self) -> list[dict]:
        """Get items for the morning digest — unsent and not yet digested."""
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT id, timestamp, source, importance, category,
                          region, title, summary, impact
                   FROM message_queue
                   WHERE sent_to_channel = 0 AND included_in_digest = 0
                   ORDER BY importance DESC, timestamp ASC"""
            ).fetchall()
            return [dict(r) for r in rows]

    def get_queue_count(self) -> dict:
        """Get queue statistics."""
        with self._connect() as conn:
            row = conn.execute(
                """SELECT
                       COUNT(*) as total,
                       SUM(CASE WHEN sent_to_channel = 0 AND included_in_digest = 0 THEN 1 ELSE 0 END) as pending,
                       SUM(CASE WHEN sent_to_channel = 1 THEN 1 ELSE 0 END) as sent,
                       SUM(CASE WHEN included_in_digest = 1 THEN 1 ELSE 0 END) as digested
                   FROM message_queue"""
            ).fetchone()
            return dict(row)

    # --- ForexFactory Events ---

    def is_ff_event_stored(self, event_hash: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM ff_events WHERE event_hash = ?",
                (event_hash,),
            ).fetchone()
            return row is not None

    def store_ff_event(
        self,
        event_hash: str,
        title: str,
        country: str,
        event_time_utc: str,
        event_time_london: str,
        forecast: str,
        previous: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO ff_events
                   (event_hash, title, country, event_time_utc,
                    event_time_london, forecast, previous, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    event_hash,
                    title,
                    country,
                    event_time_utc,
                    event_time_london,
                    forecast,
                    previous,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def get_ff_events_needing_reminder(
        self, window_start: str, window_end: str
    ) -> list[dict]:
        """Get events in the time window that haven't had reminders sent."""
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT id, event_hash, title, country, event_time_utc,
                          event_time_london, forecast, previous
                   FROM ff_events
                   WHERE reminder_sent = 0
                     AND event_time_utc >= ?
                     AND event_time_utc <= ?
                   ORDER BY event_time_utc ASC""",
                (window_start, window_end),
            ).fetchall()
            return [dict(r) for r in rows]

    def mark_ff_reminder_sent(self, event_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE ff_events SET reminder_sent = 1 WHERE id = ?",
                (event_id,),
            )

    def get_ff_upcoming_events(self, limit: int = 10) -> list[dict]:
        """Get upcoming events that haven't happened yet."""
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT title, country, event_time_london, forecast,
                          previous, reminder_sent
                   FROM ff_events
                   WHERE event_time_utc >= ?
                   ORDER BY event_time_utc ASC
                   LIMIT ?""",
                (now, limit),
            ).fetchall()
            return [dict(r) for r in rows]
