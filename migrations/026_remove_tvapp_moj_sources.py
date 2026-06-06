"""
Migration 026: remove TheTVApp (tvapp) and MoveOnJoy (moj) source rows.

The scrapers for these sources have been deleted from tvpass.py.
This migration removes their DB rows along with all attached channels,
programs, and feed_channel_numbers entries.

Safe to re-run (no-op if the rows do not exist).
"""
import sqlite3

DB_PATH = "/data/playlistmanager.db"

SOURCES_TO_REMOVE = ('tvapp', 'moj', 'thetvapp', 'thetvapp_direct', 'moveonjoy')

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

for name in SOURCES_TO_REMOVE:
    cur.execute("SELECT id FROM sources WHERE name = ?", (name,))
    row = cur.fetchone()
    if not row:
        print(f"Migration 026: '{name}' not found — skipping.")
        continue

    source_id = row[0]

    cur.execute(
        "DELETE FROM programs WHERE channel_id IN "
        "(SELECT id FROM channels WHERE source_id = ?)",
        (source_id,),
    )
    cur.execute(
        "DELETE FROM feed_channel_numbers WHERE channel_id IN "
        "(SELECT id FROM channels WHERE source_id = ?)",
        (source_id,),
    )
    cur.execute("DELETE FROM channels WHERE source_id = ?", (source_id,))
    cur.execute("DELETE FROM sources WHERE id = ?", (source_id,))

    con.commit()
    print(f"Migration 026: removed source '{name}' (id={source_id}) and all attached data.")

# Clean up any orphaned feed_channel_numbers that slipped through
cur.execute(
    "DELETE FROM feed_channel_numbers WHERE channel_id NOT IN "
    "(SELECT id FROM channels)"
)
con.commit()
con.close()
print("Migration 026 complete.")
