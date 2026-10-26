import GRDB

/// The ledjer tables, as migrations. Both the event store and the
/// accumulator apply all of them, so whichever is created first on a fresh
/// database sets up everything, and the other finds it done.
enum LedjerSchema {
    static func registerMigrations(in migrator: inout DatabaseMigrator) {
        migrator.registerMigration("ledjer-1-events") { db in
            try db.execute(sql: """
                CREATE TABLE ledjer_event (
                    position INTEGER PRIMARY KEY AUTOINCREMENT,
                    type     TEXT NOT NULL,
                    metadata BLOB NOT NULL,
                    payload  BLOB NOT NULL
                );
                CREATE INDEX ledjer_event_type ON ledjer_event (type, position);

                -- The tags of an event, as "<source>.<key>" = "<type>:<value>".
                CREATE TABLE ledjer_event_tag (
                    position INTEGER NOT NULL REFERENCES ledjer_event (position),
                    name     TEXT NOT NULL,
                    value    TEXT NOT NULL,
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
                    name     TEXT PRIMARY KEY NOT NULL,
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
