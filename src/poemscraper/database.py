import logging
import sqlite3
from pathlib import Path
from typing import Set, Optional

import aiosqlite

from .schemas import PoemSchema

logger = logging.getLogger(__name__)


def connect_sync_db(db_path: Path) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
    """Crée une connexion SQLite synchrone standard pour le thread d'écriture."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    return conn, cursor


class DatabaseManager:
    """Gère l'accès asynchrone et synchrone à la base de données d'index SQLite."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None

    async def initialize(self):
        """Initialise la connexion asynchrone et crée la table si elle n'existe pas."""
        try:
            self.conn = await aiosqlite.connect(self.db_path)
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS poems (
                    page_id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    author TEXT,
                    publication_date TEXT,
                    source_collection TEXT,
                    language TEXT NOT NULL,
                    checksum_sha256 TEXT NOT NULL,
                    extraction_timestamp TEXT NOT NULL,
                    hub_title TEXT NOT NULL,
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
        """Récupère de manière asynchrone tous les page_ids déjà dans la base de données."""
        if not self.conn:
            await self.initialize()

        assert self.conn is not None
        async with self.conn.execute("SELECT page_id FROM poems") as cursor:
            rows = await cursor.fetchall()
            return {row[0] for row in rows}

    def add_poem_index_sync(self, poem: PoemSchema, cursor: sqlite3.Cursor):
        """
        Insère de manière synchrone l'index d'un poème dans la base de données.
        Cette méthode est conçue pour être appelée depuis le thread d'écriture dédié.
        """
        cursor.execute(
            """
            INSERT OR IGNORE INTO poems (
                page_id, title, author, publication_date, source_collection,
                language, checksum_sha256, extraction_timestamp,
                hub_title, hub_page_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                poem.page_id,
                poem.title,
                poem.metadata.author,
                poem.metadata.publication_date,
                poem.metadata.source_collection,
                poem.language,
                poem.checksum_sha256,
                poem.extraction_timestamp.isoformat(),
                poem.hub_title,
                poem.hub_page_id,
            ),
        )

    async def close(self):
        """Ferme la connexion asynchrone à la base de données."""
        if self.conn:
            await self.conn.close()
            logger.info("Database connection closed.")