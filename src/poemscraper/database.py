import logging
import sqlite3
from pathlib import Path
from typing import Set

import aiosqlite

from .models import Poem, Author, PoeticCollection, VersionHub, ScrapedData

logger = logging.getLogger(__name__)


def connect_sync_db(db_path: Path) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
    """Crée une connexion SQLite synchrone standard pour le thread d'écriture."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    cursor = conn.cursor()
    return conn, cursor


class DatabaseManager:
    """Gère l'accès asynchrone et synchrone à la base de données relationnelle SQLite."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn: aiosqlite.Connection | None = None

    async def initialize(self):
        """Initialise la connexion asynchrone et crée le schéma relationnel."""
        try:
            self.conn = await aiosqlite.connect(self.db_path)
            await self.conn.execute("PRAGMA foreign_keys = ON;")
            
            await self.conn.execute("""
                CREATE TABLE IF NOT EXISTS authors (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    wikisource_url TEXT NOT NULL
                )
            """)
            
            await self.conn.execute("""
                CREATE TABLE IF NOT EXISTS collections (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    author_id INTEGER NOT NULL,
                    wikisource_url TEXT NOT NULL,
                    FOREIGN KEY (author_id) REFERENCES authors(id)
                )
            """)

            await self.conn.execute("""
                CREATE TABLE IF NOT EXISTS version_hubs (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    author_id INTEGER NOT NULL,
                    collection_id INTEGER,
                    wikisource_url TEXT NOT NULL,
                    FOREIGN KEY (author_id) REFERENCES authors(id),
                    FOREIGN KEY (collection_id) REFERENCES collections(id)
                )
            """)

            await self.conn.execute("""
                CREATE TABLE IF NOT EXISTS poems (
                    page_id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    language TEXT NOT NULL,
                    author_id INTEGER NOT NULL,
                    collection_id INTEGER,
                    hub_id INTEGER,
                    checksum_sha256 TEXT NOT NULL,
                    extraction_timestamp TEXT NOT NULL,
                    wikisource_url TEXT NOT NULL,
                    FOREIGN KEY (author_id) REFERENCES authors(id),
                    FOREIGN KEY (collection_id) REFERENCES collections(id),
                    FOREIGN KEY (hub_id) REFERENCES version_hubs(id)
                )
            """)
            
            await self.conn.commit()
            logger.info(f"Database schema initialized successfully at {self.db_path}")
        except Exception as e:
            logger.critical(f"Failed to initialize database: {e}")
            raise

    async def get_all_processed_ids(self) -> Set[int]:
        """Récupère tous les page_ids déjà traités depuis toutes les tables."""
        if not self.conn:
            await self.initialize()
        
        ids = set()
        queries = [
            "SELECT id FROM authors",
            "SELECT id FROM collections",
            "SELECT id FROM version_hubs",
            "SELECT page_id FROM poems"
        ]
        for query in queries:
            async with self.conn.execute(query) as cursor:
                rows = await cursor.fetchall()
                ids.update(row[0] for row in rows)
        return ids

    def add_scraped_data_sync(self, data: ScrapedData, cursor: sqlite3.Cursor):
        """
        Insère de manière synchrone une entité (Auteur, Recueil, Poème, etc.) dans la DB.
        Conçue pour être appelée depuis le thread d'écriture.
        """
        if isinstance(data, Author):
            cursor.execute(
                "INSERT OR IGNORE INTO authors (id, name, wikisource_url) VALUES (?, ?, ?)",
                (data.id, data.name, str(data.wikisource_url))
            )
        elif isinstance(data, PoeticCollection):
            cursor.execute(
                "INSERT OR IGNORE INTO collections (id, title, author_id, wikisource_url) VALUES (?, ?, ?, ?)",
                (data.id, data.title, data.author_id, str(data.wikisource_url))
            )
        elif isinstance(data, VersionHub):
            cursor.execute(
                "INSERT OR IGNORE INTO version_hubs (id, title, author_id, collection_id, wikisource_url) VALUES (?, ?, ?, ?, ?)",
                (data.id, data.title, data.author_id, data.collection_id, str(data.wikisource_url))
            )
        elif isinstance(data, Poem):
            cursor.execute(
                """
                INSERT OR IGNORE INTO poems (
                    page_id, title, language, author_id, collection_id, hub_id,
                    checksum_sha256, extraction_timestamp, wikisource_url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    data.page_id, data.title, data.language, data.author_id,
                    data.collection_id, data.hub_id, data.checksum_sha256,
                    data.extraction_timestamp.isoformat(), str(data.wikisource_url)
                ),
            )

    async def close(self):
        """Ferme la connexion asynchrone à la base de données."""
        if self.conn:
            await self.conn.close()
            logger.info("Database connection closed.")