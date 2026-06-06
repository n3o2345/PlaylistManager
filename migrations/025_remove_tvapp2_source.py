"""
Migration 025: remove the orphaned legacy source row for the old local-proxy scraper.

The local-proxy scraper (127.0.0.1:4124) has been removed. Any existing row
in the sources table is a leftover from a previous installation. This migration
deletes that row along with all channels and programs that were attached to it.

Safe to re-run (no-op if the row does not exist).
"""
import sqlite3

DB_PATH = "/data/playlistmanager.db"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cur.execute("SELECT id FROM sources WHERE name = 'tvapp2'")
row = cur.fetchone()
if row:
    source_id = row[0]

    # Remove programs linked to the old source's channels
    cur.execute(
        "DELETE FROM programs WHERE channel_id IN "
        "(SELECT id FROM channels WHERE source_id = ?)",
        (source_id,),
    )
    # Remove channels
    cur.execute("DELETE FROM channels WHERE source_id = ?", (source_id,))
    # Clean up any orphaned feed_channel_numbers entries
    cur.execute(
        "DELETE FROM feed_channel_numbers WHERE channel_id NOT IN "
        "(SELECT id FROM channels)"
    )
    # Remove the source row itself
    cur.execute("DELETE FROM sources WHERE id = ?", (source_id,))

    con.commit()
    print(f"Migration 025 done — removed legacy source (id={source_id}) and its channels/programs.")
else:
    print("Migration 025 skipped — legacy source row not found.")

con.close()
