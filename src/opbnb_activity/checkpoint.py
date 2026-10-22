import sqlite3


class Checkpoint:
    def __init__(self, path, identity):
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("CREATE TABLE IF NOT EXISTS metadata (identity TEXT NOT NULL)")
        self.connection.execute("CREATE TABLE IF NOT EXISTS tasks (block_number INTEGER PRIMARY KEY, end_block INTEGER NOT NULL, path TEXT NOT NULL, checksum TEXT NOT NULL, status TEXT NOT NULL CHECK(status='complete'))")
        row = self.connection.execute("SELECT identity FROM metadata").fetchone()
        if row and row["identity"] != identity:
            self.connection.close()
            raise ValueError("checkpoint identity mismatch")
        if row is None:
            self.connection.execute("INSERT INTO metadata VALUES (?)", (identity,))
        self.connection.commit()

    def get(self, start_block):
        row = self.connection.execute("SELECT * FROM tasks WHERE block_number=?", (start_block,)).fetchone()
        return dict(row) if row else None

    def complete(self, start_block, end_block, path, checksum):
        with self.connection:
            self.connection.execute("INSERT OR REPLACE INTO tasks VALUES (?,?,?,?, 'complete')", (start_block, end_block, path, checksum))

    def close(self):
        self.connection.close()
