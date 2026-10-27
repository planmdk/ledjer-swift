import Foundation
import GRDB

/// The ledjer tables, as migrations. Both the event store and the
/// accumulator apply all of them, so whichever is created first on a fresh
/// database sets up everything, and the other finds it done.
enum LedjerSchema {
    /// The form in which every compared string (event types, tag names and values, checkpoint names)
    /// is bound and stored: its UTF-8 bytes, as a BLOB. Bound as TEXT, GRDB's `sqlite3_bind_text(…, -1, …)`
    /// would cut a string at its first NUL character, so that `"a\0b"` and `"a"` were the same value.
    /// Like the Clojure library, the store compares strings exactly.
    static func blob(_ string: String) -> Data { Data(string.utf8) }

    /// The inverse of `blob(_:)`.
    static func string(_ blob: Data) -> String { String(decoding: blob, as: UTF8.self) }

    static func registerMigrations(in migrator: inout DatabaseMigrator) {
        migrator.registerMigration("ledjer-1-events") { db in
            try db.execute(sql: """
                CREATE TABLE ledjer_event (
                    position INTEGER PRIMARY KEY AUTOINCREMENT,
                    type     BLOB NOT NULL,  -- UTF-8, see LedjerSchema.blob
                    metadata BLOB NOT NULL,
                    payload  BLOB NOT NULL
                );
                CREATE INDEX ledjer_event_type ON ledjer_event (type, position);

                -- The tags of an event, as "<source>.<key>" = "<type>:<value>".
                CREATE TABLE ledjer_event_tag (
                    position INTEGER NOT NULL REFERENCES ledjer_event (position),
                    name     BLOB NOT NULL,  -- UTF-8, see LedjerSchema.blob
                    value    BLOB NOT NULL,  -- UTF-8
                    PRIMARY KEY (position, name)
                ) WITHOUT ROWID;
                CREATE INDEX ledjer_event_tag_lookup ON ledjer_event_tag (name, value, position);

                -- The log is append-only. Tags are part of it: editing one would change what queries return.
                CREATE TRIGGER ledjer_event_no_update BEFORE UPDATE ON ledjer_event
                BEGIN SELECT RAISE(ABORT, 'ledjer events are immutable'); END;
                CREATE TRIGGER ledjer_event_no_delete BEFORE DELETE ON ledjer_event
                BEGIN SELECT RAISE(ABORT, 'ledjer events are immutable'); END;
                CREATE TRIGGER ledjer_event_tag_no_update BEFORE UPDATE ON ledjer_event_tag
                BEGIN SELECT RAISE(ABORT, 'ledjer events are immutable'); END;
                CREATE TRIGGER ledjer_event_tag_no_delete BEFORE DELETE ON ledjer_event_tag
                BEGIN SELECT RAISE(ABORT, 'ledjer events are immutable'); END;
                """)
        }

        migrator.registerMigration("ledjer-2-checkpoint") { db in
            // Where persisted views (see GRDBAccumulator) have got to.
            try db.execute(sql: """
                CREATE TABLE ledjer_checkpoint (
                    name     BLOB PRIMARY KEY NOT NULL,  -- UTF-8, see LedjerSchema.blob
                    position INTEGER NOT NULL
                );
                """)
        }
    }

    static func migrate(_ writer: any DatabaseWriter) throws {
        var migrator = DatabaseMigrator()
        registerMigrations(in: &migrator)
        try migrator.migrate(writer)
    }
}
