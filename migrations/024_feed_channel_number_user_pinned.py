"""
Migration 024: add user_pinned column to feed_channel_numbers.

Allows users to manually pin a specific channel number for a channel within
a particular feed, overriding the auto-assigned sequential number.

Safe to re-run (ALTER TABLE … IF NOT EXISTS equivalent via try/except).
"""
import sqlite3

DB_PATH = "/data/playlistmanager.db"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# SQLite doesn't support IF NOT EXISTS on ALTER TABLE, so we check first.
cur.execute("PRAGMA table_info(feed_channel_numbers)")
cols = {row[1] for row in cur.fetchall()}
if 'user_pinned' not in cols:
    cur.execute("ALTER TABLE feed_channel_numbers ADD COLUMN user_pinned INTEGER NOT NULL DEFAULT 0")
    print("Migration 024 done — added user_pinned column to feed_channel_numbers.")
else:
    print("Migration 024 skipped — user_pinned column already exists.")

con.commit()
con.close()
