from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import aiosqlite

logger = logging.getLogger(__name__)


SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS subjects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  telegram_thread_id TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS subject_preferences (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subject_id INTEGER NOT NULL,
  version INTEGER NOT NULL,
  include_rules_json TEXT NOT NULL,
  exclude_rules_json TEXT NOT NULL,
  source_weights_json TEXT NOT NULL,
  quality_rules_json TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(subject_id) REFERENCES subjects(id)
);

CREATE TABLE IF NOT EXISTS sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  platform TEXT NOT NULL,
  account_or_channel_id TEXT NOT NULL,
  display_name TEXT NOT NULL,
  follow_state TEXT NOT NULL DEFAULT 'active',
  last_crawled_at TEXT,
  UNIQUE(platform, account_or_channel_id)
);

CREATE TABLE IF NOT EXISTS discovered_sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  platform TEXT NOT NULL,
  account_or_channel_id TEXT NOT NULL,
  display_name TEXT NOT NULL,
  discovery_type TEXT NOT NULL,
  evidence_json TEXT NOT NULL,
  confidence_score REAL NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS subject_sources (
  subject_id INTEGER NOT NULL,
  source_id INTEGER NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'active',
  PRIMARY KEY(subject_id, source_id),
  FOREIGN KEY(subject_id) REFERENCES subjects(id),
  FOREIGN KEY(source_id) REFERENCES sources(id)
);

CREATE TABLE IF NOT EXISTS items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  platform TEXT NOT NULL,
  external_id TEXT NOT NULL,
  canonical_url TEXT,
  author TEXT,
  published_at TEXT,
  raw_text TEXT,
  transcript_text TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  UNIQUE(platform, external_id)
);

CREATE TABLE IF NOT EXISTS item_segments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  item_id INTEGER NOT NULL,
  start_offset INTEGER NOT NULL,
  end_offset INTEGER NOT NULL,
  segment_text TEXT NOT NULL,
  segment_type TEXT NOT NULL,
  FOREIGN KEY(item_id) REFERENCES items(id)
);

CREATE TABLE IF NOT EXISTS item_scores (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  item_id INTEGER NOT NULL,
  subject_id INTEGER NOT NULL,
  pass1_score REAL,
  pass2_score REAL,
  practicality_score REAL,
  novelty_score REAL,
  trust_score REAL,
  noise_penalty REAL,
  final_score REAL,
  rationale_json TEXT NOT NULL DEFAULT '{}',
  FOREIGN KEY(item_id) REFERENCES items(id),
  FOREIGN KEY(subject_id) REFERENCES subjects(id),
  UNIQUE(item_id, subject_id)
);

CREATE TABLE IF NOT EXISTS digests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subject_id INTEGER NOT NULL,
  run_date TEXT NOT NULL,
  sent_at TEXT,
  status TEXT NOT NULL,
  FOREIGN KEY(subject_id) REFERENCES subjects(id)
);

CREATE TABLE IF NOT EXISTS digest_items (
  digest_id INTEGER NOT NULL,
  item_id INTEGER NOT NULL,
  rank INTEGER NOT NULL,
  reason_selected TEXT NOT NULL,
  PRIMARY KEY(digest_id, item_id),
  FOREIGN KEY(digest_id) REFERENCES digests(id),
  FOREIGN KEY(item_id) REFERENCES items(id)
);

CREATE TABLE IF NOT EXISTS feedback_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subject_id INTEGER NOT NULL,
  item_id INTEGER,
  feedback_type TEXT NOT NULL,
  comment_text TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(subject_id) REFERENCES subjects(id),
  FOREIGN KEY(item_id) REFERENCES items(id)
);

CREATE TABLE IF NOT EXISTS run_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_type TEXT NOT NULL,
  started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ended_at TEXT,
  status TEXT NOT NULL,
  stats_json TEXT NOT NULL DEFAULT '{}'
);
"""


MIGRATIONS: list[tuple[int, str]] = [
    (1, SCHEMA_V1),
    (
        2,
        """
        CREATE TABLE IF NOT EXISTS telegram_chats (
          chat_id INTEGER PRIMARY KEY,
          title TEXT,
          last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS subject_routes (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject_id INTEGER NOT NULL,
          chat_id INTEGER NOT NULL,
          thread_id TEXT,
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY(subject_id) REFERENCES subjects(id),
          FOREIGN KEY(chat_id) REFERENCES telegram_chats(chat_id),
          UNIQUE(subject_id, chat_id, thread_id)
        );
        """,
    ),
    (
        3,
        """
        ALTER TABLE items ADD COLUMN ingested_at TEXT;
        ALTER TABLE items ADD COLUMN updated_at TEXT;
        ALTER TABLE items ADD COLUMN content_hash TEXT;

        UPDATE items
        SET
          ingested_at = COALESCE(ingested_at, CURRENT_TIMESTAMP),
          updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP);

        DELETE FROM subject_routes
        WHERE id NOT IN (
          SELECT MIN(id)
          FROM subject_routes
          GROUP BY subject_id, chat_id, COALESCE(thread_id, '')
        );

        UPDATE subject_routes
        SET thread_id = ''
        WHERE thread_id IS NULL;

        DELETE FROM digests
        WHERE id NOT IN (
          SELECT MIN(id)
          FROM digests
          GROUP BY subject_id, run_date
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_digests_subject_run_date
          ON digests(subject_id, run_date);

        CREATE TABLE IF NOT EXISTS digest_deliveries (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          digest_id INTEGER NOT NULL,
          chat_id INTEGER NOT NULL,
          thread_id TEXT NOT NULL DEFAULT '',
          message_id INTEGER,
          sent_at TEXT,
          status TEXT NOT NULL,
          error_text TEXT,
          FOREIGN KEY(digest_id) REFERENCES digests(id),
          UNIQUE(digest_id, chat_id, thread_id)
        );

        CREATE TABLE IF NOT EXISTS digest_buttons (
          token TEXT PRIMARY KEY,
          digest_id INTEGER NOT NULL,
          item_id INTEGER NOT NULL,
          subject_id INTEGER NOT NULL,
          action TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY(digest_id) REFERENCES digests(id),
          FOREIGN KEY(item_id) REFERENCES items(id),
          FOREIGN KEY(subject_id) REFERENCES subjects(id)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_feedback_subject_item_type
          ON feedback_events(subject_id, item_id, feedback_type)
          WHERE item_id IS NOT NULL;

        CREATE INDEX IF NOT EXISTS idx_item_scores_subject_score
          ON item_scores(subject_id, final_score DESC);

        CREATE INDEX IF NOT EXISTS idx_subject_routes_chat_thread
          ON subject_routes(chat_id, thread_id);
        """,
    ),
    (
        4,
        """
        CREATE TABLE IF NOT EXISTS onboarding_state (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          current_step TEXT NOT NULL DEFAULT 'start',
          timezone TEXT,
          digest_time TEXT,
          telegram_verified INTEGER NOT NULL DEFAULT 0,
          subject_name TEXT,
          include_terms_json TEXT NOT NULL DEFAULT '[]',
          exclude_terms_json TEXT NOT NULL DEFAULT '[]',
          high_quality_examples TEXT,
          completed_at TEXT,
          updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        INSERT INTO onboarding_state(id)
        VALUES (1)
        ON CONFLICT(id) DO NOTHING;

        CREATE TABLE IF NOT EXISTS onboarding_imported_sources (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          platform TEXT NOT NULL,
          account_or_channel_id TEXT NOT NULL,
          display_name TEXT NOT NULL,
          raw_source TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          UNIQUE(platform, account_or_channel_id)
        );

        CREATE INDEX IF NOT EXISTS idx_onboarding_imported_sources_status
          ON onboarding_imported_sources(status, platform);
        """,
    ),
    (
        5,
        """
        ALTER TABLE sources ADD COLUMN is_monitored INTEGER NOT NULL DEFAULT 1;

        UPDATE sources
        SET is_monitored = 1
        WHERE id IN (
          SELECT source_id
          FROM subject_sources
          WHERE status = 'active'
        );

        CREATE INDEX IF NOT EXISTS idx_sources_monitored_state
          ON sources(is_monitored, follow_state, platform);
        """,
    ),
    (
        6,
        """
        CREATE TABLE IF NOT EXISTS pending_subject_drafts (
          chat_id INTEGER PRIMARY KEY,
          title TEXT NOT NULL,
          description_text TEXT NOT NULL,
          extracted_rules_json TEXT NOT NULL,
          last_user_message TEXT NOT NULL,
          updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_pending_subject_drafts_updated_at
          ON pending_subject_drafts(updated_at);
        """,
    ),
    (
        7,
        """
        ALTER TABLE subjects ADD COLUMN button_shortcuts_json TEXT;

        ALTER TABLE digest_items ADD COLUMN short_text TEXT;
        ALTER TABLE digest_items ADD COLUMN full_text TEXT;

        ALTER TABLE digest_buttons ADD COLUMN label TEXT;
        ALTER TABLE digest_buttons ADD COLUMN kind TEXT NOT NULL DEFAULT 'feedback';

        CREATE TABLE IF NOT EXISTS digest_item_deliveries (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          digest_id INTEGER NOT NULL,
          item_id INTEGER NOT NULL,
          chat_id INTEGER NOT NULL,
          thread_id TEXT NOT NULL DEFAULT '',
          message_id INTEGER,
          sent_at TEXT,
          status TEXT NOT NULL,
          error_text TEXT,
          FOREIGN KEY(digest_id) REFERENCES digests(id),
          FOREIGN KEY(item_id) REFERENCES items(id)
        );

        CREATE INDEX IF NOT EXISTS idx_digest_item_deliveries_message
          ON digest_item_deliveries(chat_id, message_id);

        DROP INDEX IF EXISTS idx_feedback_subject_item_type;
        """,
    ),
]


@dataclass
class Database:
    path: Path
    conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA foreign_keys = ON;")

    async def close(self) -> None:
        if self.conn is not None:
            await self.conn.close()
            self.conn = None

    async def initialize(self) -> None:
        if self.conn is None:
            raise RuntimeError("Database is not connected.")

        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
              version INTEGER PRIMARY KEY,
              applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        await self.conn.commit()

        row = await (await self.conn.execute("SELECT COALESCE(MAX(version), 0) AS v FROM schema_migrations")).fetchone()
        current_version = int(row["v"])
        for version, sql in MIGRATIONS:
            if version <= current_version:
                continue
            logger.info("Applying migration v%s", version)
            await self.conn.executescript(sql)
            await self.conn.execute("INSERT INTO schema_migrations(version) VALUES (?)", (version,))
            await self.conn.commit()
