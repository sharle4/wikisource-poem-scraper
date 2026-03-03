"""
Builds a local SQLite index from Wikimedia SQL dump files.

This is the first phase of offline processing: parse the SQL dumps
(page, linktarget, categorylinks, redirect) into a fast-lookup SQLite
database, then use it for category tree traversal, redirect resolution,
and title→page_id mapping.
"""
import logging
import sqlite3
from collections import deque
from pathlib import Path
from typing import Dict, Optional, Set

from .dump_readers import (
    iter_categorylinks_rows,
    iter_linktarget_rows,
    iter_page_rows,
    iter_redirect_rows,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 50_000


class DumpIndexBuilder:
    """
    Parses SQL dump files and builds a local SQLite index database.
    """

    def __init__(self, dumps_dir: Path, index_db_path: Path):
        self.dumps_dir = dumps_dir
        self.index_db_path = index_db_path

    def build(self) -> sqlite3.Connection:
        """
        Builds the complete index. Returns the open connection.
        """
        logger.info(f"Building dump index at {self.index_db_path}")

        conn = sqlite3.connect(str(self.index_db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA cache_size=-512000")

        # 1. Create tables (no indexes yet — added after bulk inserts)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS pages (
                page_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                namespace INTEGER NOT NULL,
                is_redirect INTEGER NOT NULL DEFAULT 0,
                page_len INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS linktarget (
                lt_id INTEGER PRIMARY KEY,
                lt_namespace INTEGER NOT NULL,
                lt_title TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS categorylinks (
                cl_from INTEGER NOT NULL,
                cl_target_id INTEGER NOT NULL,
                cl_type TEXT NOT NULL DEFAULT 'page'
            );

            CREATE TABLE IF NOT EXISTS redirects (
                rd_from INTEGER PRIMARY KEY,
                rd_namespace INTEGER NOT NULL,
                rd_title TEXT NOT NULL
            );
        """)
        conn.commit()

        # 2. Parse linktarget.sql
        lt_path = self.dumps_dir / "frwikisource-latest-linktarget.sql"
        logger.info(f"Parsing {lt_path.name}...")
        conn.execute("BEGIN TRANSACTION")
        batch = []
        for row in iter_linktarget_rows(lt_path):
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                conn.executemany(
                    "INSERT OR IGNORE INTO linktarget (lt_id, lt_namespace, lt_title) VALUES (?, ?, ?)",
                    batch,
                )
                batch.clear()
        if batch:
            conn.executemany(
                "INSERT OR IGNORE INTO linktarget (lt_id, lt_namespace, lt_title) VALUES (?, ?, ?)",
                batch,
            )
        conn.commit()
        logger.info("linktarget table populated.")

        # 3. Parse page.sql
        page_path = self.dumps_dir / "frwikisource-latest-page.sql"
        logger.info(f"Parsing {page_path.name}...")
        conn.execute("BEGIN TRANSACTION")
        batch = []
        for row in iter_page_rows(page_path):
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                conn.executemany(
                    "INSERT OR IGNORE INTO pages (page_id, namespace, title, is_redirect, page_len) VALUES (?, ?, ?, ?, ?)",
                    batch,
                )
                batch.clear()
        if batch:
            conn.executemany(
                "INSERT OR IGNORE INTO pages (page_id, namespace, title, is_redirect, page_len) VALUES (?, ?, ?, ?, ?)",
                batch,
            )
        conn.commit()
        logger.info("pages table populated.")

        # 4. Parse categorylinks.sql
        cl_path = self.dumps_dir / "frwikisource-latest-categorylinks.sql"
        logger.info(f"Parsing {cl_path.name}...")
        conn.execute("BEGIN TRANSACTION")
        batch = []
        for row in iter_categorylinks_rows(cl_path):
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                conn.executemany(
                    "INSERT OR IGNORE INTO categorylinks (cl_from, cl_type, cl_target_id) VALUES (?, ?, ?)",
                    batch,
                )
                batch.clear()
        if batch:
            conn.executemany(
                "INSERT OR IGNORE INTO categorylinks (cl_from, cl_type, cl_target_id) VALUES (?, ?, ?)",
                batch,
            )
        conn.commit()
        logger.info("categorylinks table populated.")

        # 5. Parse redirect.sql
        rd_path = self.dumps_dir / "frwikisource-latest-redirect.sql"
        logger.info(f"Parsing {rd_path.name}...")
        conn.execute("BEGIN TRANSACTION")
        batch = []
        for row in iter_redirect_rows(rd_path):
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                conn.executemany(
                    "INSERT OR IGNORE INTO redirects (rd_from, rd_namespace, rd_title) VALUES (?, ?, ?)",
                    batch,
                )
                batch.clear()
        if batch:
            conn.executemany(
                "INSERT OR IGNORE INTO redirects (rd_from, rd_namespace, rd_title) VALUES (?, ?, ?)",
                batch,
            )
        conn.commit()
        logger.info("redirects table populated.")

        # 6. Create indexes AFTER bulk inserts
        logger.info("Creating indexes...")
        conn.executescript("""
            CREATE INDEX IF NOT EXISTS idx_lt_ns_title ON linktarget(lt_namespace, lt_title);
            CREATE INDEX IF NOT EXISTS idx_cl_target ON categorylinks(cl_target_id, cl_type);
            CREATE INDEX IF NOT EXISTS idx_cl_from ON categorylinks(cl_from);
            CREATE INDEX IF NOT EXISTS idx_pages_ns_title ON pages(namespace, title);
        """)
        conn.commit()

        # 7. VACUUM and ANALYZE
        logger.info("Running VACUUM and ANALYZE...")
        conn.execute("VACUUM")
        conn.execute("ANALYZE")

        logger.info("Dump index build complete.")
        return conn

    def find_pages_in_category_tree(
        self, conn: sqlite3.Connection, root_category_title: str, lang: str
    ) -> Set[int]:
        """
        Starting from a root category (e.g., "Poèmes"), recursively traverses
        the category tree and returns ALL page_ids that belong to any
        descendant category.

        Category titles are stored with underscores in linktarget.
        """
        # Normalize: replace spaces with underscores for the DB lookup
        root_title_underscored = root_category_title.replace(" ", "_")

        # Find the root category's lt_id
        row = conn.execute(
            "SELECT lt_id FROM linktarget WHERE lt_namespace = 14 AND lt_title = ?",
            (root_title_underscored,),
        ).fetchone()

        if row is None:
            # Try with spaces too (some dumps may store it differently)
            row = conn.execute(
                "SELECT lt_id FROM linktarget WHERE lt_namespace = 14 AND lt_title = ?",
                (root_category_title,),
            ).fetchone()

        if row is None:
            logger.error(
                f"Root category '{root_category_title}' not found in linktarget table."
            )
            return set()

        root_lt_id = row[0]
        logger.info(f"Root category '{root_category_title}' has lt_id={root_lt_id}")

        # BFS to traverse all subcategories
        visited_lt_ids: Set[int] = {root_lt_id}
        queue: deque[int] = deque([root_lt_id])
        all_category_lt_ids: Set[int] = {root_lt_id}

        while queue:
            current_lt_id = queue.popleft()

            # Find all subcategory page_ids under this category
            subcat_page_ids = conn.execute(
                "SELECT cl_from FROM categorylinks WHERE cl_target_id = ? AND cl_type = 'subcat'",
                (current_lt_id,),
            ).fetchall()

            for (subcat_page_id,) in subcat_page_ids:
                # Get the subcategory's title from pages table
                title_row = conn.execute(
                    "SELECT title FROM pages WHERE page_id = ? AND namespace = 14",
                    (subcat_page_id,),
                ).fetchone()

                if title_row is None:
                    continue

                subcat_title = title_row[0].replace(" ", "_")

                # Find the lt_id for this subcategory title
                lt_row = conn.execute(
                    "SELECT lt_id FROM linktarget WHERE lt_namespace = 14 AND lt_title = ?",
                    (subcat_title,),
                ).fetchone()

                if lt_row is None:
                    # Try with spaces
                    lt_row = conn.execute(
                        "SELECT lt_id FROM linktarget WHERE lt_namespace = 14 AND lt_title = ?",
                        (title_row[0],),
                    ).fetchone()

                if lt_row is None:
                    continue

                subcat_lt_id = lt_row[0]
                if subcat_lt_id not in visited_lt_ids:
                    visited_lt_ids.add(subcat_lt_id)
                    all_category_lt_ids.add(subcat_lt_id)
                    queue.append(subcat_lt_id)

        logger.info(
            f"Category tree traversal found {len(all_category_lt_ids)} categories."
        )

        # Collect all page_ids from all discovered categories (type = 'page')
        all_page_ids: Set[int] = set()
        lt_id_list = list(all_category_lt_ids)

        for i in range(0, len(lt_id_list), 500):
            chunk = lt_id_list[i : i + 500]
            placeholders = ",".join("?" * len(chunk))
            rows = conn.execute(
                f"SELECT cl_from FROM categorylinks WHERE cl_target_id IN ({placeholders}) AND cl_type = 'page'",
                chunk,
            ).fetchall()
            all_page_ids.update(row[0] for row in rows)

        # Filter to only ns:0 pages
        if all_page_ids:
            ns0_page_ids: Set[int] = set()
            page_id_list = list(all_page_ids)
            for i in range(0, len(page_id_list), 500):
                chunk = page_id_list[i : i + 500]
                placeholders = ",".join("?" * len(chunk))
                rows = conn.execute(
                    f"SELECT page_id FROM pages WHERE page_id IN ({placeholders}) AND namespace = 0",
                    chunk,
                ).fetchall()
                ns0_page_ids.update(row[0] for row in rows)
            all_page_ids = ns0_page_ids

        logger.info(
            f"Category tree contains {len(all_page_ids)} ns:0 pages."
        )
        return all_page_ids

    def get_categories_for_page(
        self, conn: sqlite3.Connection, page_id: int
    ) -> Set[str]:
        """
        Returns the set of category titles (human-readable, with spaces)
        for a given page_id.
        """
        rows = conn.execute(
            """
            SELECT lt.lt_title FROM categorylinks cl
            JOIN linktarget lt ON cl.cl_target_id = lt.lt_id
            WHERE cl.cl_from = ? AND lt.lt_namespace = 14
            """,
            (page_id,),
        ).fetchall()
        return {row[0].replace("_", " ") for row in rows}

    def resolve_redirect(
        self, conn: sqlite3.Connection, page_id: int, max_depth: int = 5
    ) -> Optional[int]:
        """
        If page_id is a redirect, resolves to the final target page_id.
        Follows up to max_depth redirect hops.
        Returns None if not a redirect or target can't be found.
        """
        current_id = page_id
        visited: Set[int] = set()

        for _ in range(max_depth):
            if current_id in visited:
                logger.warning(f"Circular redirect detected at page_id {current_id}")
                return None
            visited.add(current_id)

            # Check if current page is a redirect
            row = conn.execute(
                "SELECT is_redirect FROM pages WHERE page_id = ?", (current_id,)
            ).fetchone()

            if row is None or row[0] == 0:
                # Not a redirect (or page doesn't exist)
                return current_id if current_id != page_id else None

            # Get redirect target
            rd_row = conn.execute(
                "SELECT rd_namespace, rd_title FROM redirects WHERE rd_from = ?",
                (current_id,),
            ).fetchone()

            if rd_row is None:
                return None

            rd_namespace, rd_title = rd_row
            # Resolve target title to page_id
            target_title = rd_title.replace("_", " ")
            target_row = conn.execute(
                "SELECT page_id FROM pages WHERE title = ? AND namespace = ?",
                (target_title, rd_namespace),
            ).fetchone()

            if target_row is None:
                # Try with underscores
                target_row = conn.execute(
                    "SELECT page_id FROM pages WHERE title = ? AND namespace = ?",
                    (rd_title, rd_namespace),
                ).fetchone()

            if target_row is None:
                logger.debug(
                    f"Redirect target not found: ns={rd_namespace}, title='{rd_title}'"
                )
                return None

            current_id = target_row[0]

        logger.warning(f"Redirect chain too deep (>{max_depth}) starting from page_id {page_id}")
        return None

    def build_title_to_id_index(
        self, conn: sqlite3.Connection, namespace: int = 0
    ) -> Dict[str, int]:
        """
        Builds a title→page_id mapping for a given namespace.
        Titles have underscores converted to spaces.
        """
        rows = conn.execute(
            "SELECT page_id, title FROM pages WHERE namespace = ?",
            (namespace,),
        ).fetchall()
        return {row[1].replace("_", " "): row[0] for row in rows}
