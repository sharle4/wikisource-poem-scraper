"""
Manages the SQLite database for indexing poems and tracking progress.

Uses aiosqlite for asynchronous database operations, which is crucial for not
blocking the event loop while performing I/O.
"""
import logging
import aiosqlite
from .schemas import PoemSchema

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Handles all database interactions."""
    def __init__(self, db_path):
        self.db_path = db_path
        self._conn = None

    async def initialize(self):
        """Establishes the database connection and creates tables if they don't exist."""
        try:
            self._conn = await aiosqlite.connect(self.db_path)
            await self._conn.execute("PRAGMA journal_mode=WAL;")
            
            await self._conn.execute("""
                CREATE TABLE IF NOT EXISTS poems (
                    page_id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    author TEXT,
                    language TEXT NOT NULL,
                    checksum_sha256 TEXT NOT NULL,
                    extraction_timestamp TEXT NOT NULL
                )
            """)
            await self._conn.commit()
            logger.info(f"Database initialized successfully at {self.db_path}")
        except aiosqlite.Error as e:
            logger.critical(f"Failed to initialize database: {e}")
            raise

    async def insert_poem(self, poem_data: PoemSchema):
        """Inserts a single poem record into the index."""
        if not self._conn:
            raise ConnectionError("Database is not connected.")
        try:
            await self._conn.execute(
                """
                INSERT OR REPLACE INTO poems (page_id, title, author, language, checksum_sha256, extraction_timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    poem_data.page_id,
                    poem_data.title,
                    poem_data.author,
                    poem_data.language,
                    poem_data.checksum_sha256,
                    poem_data.extraction_timestamp.isoformat(),
                ),
            )
            await self._conn.commit()
        except aiosqlite.Error as e:
            logger.error(f"Failed to insert poem with page_id {poem_data.page_id}: {e}")

    async def get_all_processed_ids(self) -> set[int]:
        """Retrieves all page_ids currently in the database for the 'resume' feature."""
        if not self._conn:
            raise ConnectionError("Database is not connected.")
        try:
            async with self._conn.execute("SELECT page_id FROM poems") as cursor:
                rows = await cursor.fetchall()
                return {row[0] for row in rows}
        except aiosqlite.Error as e:
            logger.error(f"Failed to fetch processed IDs: {e}")
            return set()

    async def close(self):
        """Closes the database connection."""
        if self._conn:
            await self._conn.close()
            logger.info("Database connection closed.")