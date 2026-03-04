import logging
import sqlite3
from pathlib import Path
from typing import Set, Optional

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
        self.conn: Optional[aiosqlite.Connection] = None

    async def initialize(self):
        """Initializes the asynchronous connection and creates/updates the table."""
        try:
            self.conn = await aiosqlite.connect(self.db_path)
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS poems (
                    page_id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    author TEXT,
                    publication_date TEXT,
                    language TEXT NOT NULL,
                    checksum_sha256 TEXT NOT NULL,
                    extraction_timestamp TEXT NOT NULL,

                    collection_page_id INTEGER,
                    collection_title TEXT,
                    section_title TEXT,
                    poem_order INTEGER,

                    hub_title TEXT,
                    hub_page_id INTEGER NOT NULL
                )
            """
            )
            await self.conn.commit()
            logger.info(f"Database initialized successfully at {self.db_path}")
        except Exception as e:
            logger.critical(f"Failed to initialize database: {e}")
            raise

    async def get_all_processed_ids(self) -> Set[int]:
        """Asynchronously retrieves all page_ids already in the database."""
        if not self.conn:
            await self.initialize()

        assert self.conn is not None
        async with self.conn.execute("SELECT page_id FROM poems") as cursor:
            rows = await cursor.fetchall()
            return {row[0] for row in rows}

    def add_poem_index_sync(self, poem: PoemSchema, cursor: sqlite3.Cursor):
        """
        Synchronously inserts or replaces a poem's index.
        The replacement is needed so that the version with context (processed later)
        can overwrite a version without context (processed earlier due to race conditions).
        """
        cursor.execute(
            """
            INSERT OR REPLACE INTO poems (
                page_id, title, author, publication_date, language,
                checksum_sha256, extraction_timestamp,
                collection_page_id, collection_title, section_title, poem_order,
                hub_title, hub_page_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                poem.page_id,
                poem.title,
                poem.metadata.author,
                poem.metadata.publication_date,
                poem.language,
                poem.checksum_sha256,
                poem.extraction_timestamp.isoformat(),
                poem.collection_page_id,
                poem.collection_title,
                poem.section_title,
                poem.poem_order,
                poem.hub_title,
                poem.hub_page_id,
            ),
        )

    def initialize_sync(self) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
        """Synchronous initialization for offline mode.
        Creates the poems table and returns a (connection, cursor) pair."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS poems (
                page_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                author TEXT,
                publication_date TEXT,
                language TEXT NOT NULL,
                checksum_sha256 TEXT NOT NULL,
                extraction_timestamp TEXT NOT NULL,

                collection_page_id INTEGER,
                collection_title TEXT,
                section_title TEXT,
                poem_order INTEGER,

                hub_title TEXT,
                hub_page_id INTEGER NOT NULL
            )
            """
        )
        conn.commit()
        logger.info(f"Database initialized (sync) at {self.db_path}")
        return conn, cursor

    def get_all_processed_ids_sync(self) -> Set[int]:
        """Synchronous version of get_all_processed_ids for offline resume mode."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("SELECT page_id FROM poems")
        ids = {row[0] for row in cursor.fetchall()}
        conn.close()
        return ids

    async def close(self):
        """Closes the asynchronous database connection."""
        if self.conn:
            await self.conn.close()
            logger.info("Database connection closed.")
