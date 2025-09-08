import logging
import sqlite3
from pathlib import Path
from typing import Set

import aiosqlite

from .schemas import PoemSchema

logger = logging.getLogger(__name__)

def connect_sync_db(db_path: Path) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
    """Creates a standard synchronous SQLite connection for the writer thread."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    return conn, cursor

class DatabaseManager:
    """Manages asynchronous and synchronous access to the SQLite index database."""
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn: aiosqlite.Connection | None = None

    async def initialize(self):
        """Initializes the async connection and creates the table if it doesn't exist."""
        try:
            self.conn = await aiosqlite.connect(self.db_path)
            await self.conn.execute("""
                CREATE TABLE IF NOT EXISTS poems (
                    page_id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    author TEXT,
                    language TEXT NOT NULL,
                    checksum_sha256 TEXT NOT NULL,
                    extraction_timestamp TEXT NOT NULL
                )
            """)
            await self.conn.commit()
            logger.info(f"Database initialized successfully at {self.db_path}")
        except Exception as e:
            logger.critical(f"Failed to initialize database: {e}")
            raise

    async def get_all_processed_ids(self) -> Set[int]:
        """Asynchronously fetches all page_ids already in the database."""
        if not self.conn:
            await self.initialize()
        
        async with self.conn.execute("SELECT page_id FROM poems") as cursor:
            rows = await cursor.fetchall()
            return {row[0] for row in rows}

    def add_poem_index_sync(self, poem: PoemSchema, cursor: sqlite3.Cursor):
        """
        Synchronously inserts a poem's index into the database.
        This method is designed to be called from the dedicated writer thread.
        """
        cursor.execute(
            """
            INSERT OR IGNORE INTO poems (page_id, title, author, language, checksum_sha256, extraction_timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                poem.page_id,
                poem.title,
                poem.author,
                poem.language,
                poem.checksum_sha256,
                poem.extraction_timestamp.isoformat(),
            )
        )

    async def close(self):
        """Closes the asynchronous database connection."""
        if self.conn:
            await self.conn.close()
            logger.info("Database connection closed.")