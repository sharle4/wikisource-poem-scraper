import logging
import csv
import re
from pathlib import Path
from threading import Lock
from typing import List

logger = logging.getLogger(__name__)

def _sanitize_filename(name: str) -> str:
    """Nettoie une chaîne pour en faire un nom de fichier valide."""
    name = name.replace("'", "_").replace('"', "_")
    name = re.sub(r'[<>:"/\\|?*]', "_", name)
    name = re.sub(r"\s+", "_", name)
    return name

class LogManager:
    """Centralizes the management of all logging for the application."""

    def __init__(self, log_dir: Path):
        self.log_dir = log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        log_path = self.log_dir / "scraper.log"
        root_logger = logging.getLogger()
        existing_handler = None
        for h in root_logger.handlers:
            if isinstance(h, logging.FileHandler):
                try:
                    if Path(getattr(h, 'baseFilename', '')) == log_path:
                        existing_handler = h
                        break
                except Exception:
                    continue
        if existing_handler is None:
            self.file_handler = logging.FileHandler(log_path, mode='w', encoding='utf-8')
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            self.file_handler.setFormatter(formatter)
            root_logger.addHandler(self.file_handler)
        else:
            self.file_handler = existing_handler

        self.debug_html_dir = self.log_dir / "debug_html"
        self.debug_html_dir.mkdir(exist_ok=True)

        self._setup_csv_loggers()

    def _setup_csv_loggers(self):
        """Initializes CSV files with their headers."""
        self.other_log_path = self.log_dir / "other_pages.csv"
        self._write_csv_header(self.other_log_path, ["Timestamp", "PageTitle", "URL", "ParentTitle", "Reason"])

        self.collections_log_path = self.log_dir / "collections.csv"
        self._write_csv_header(self.collections_log_path, ["Timestamp", "PageTitle", "URL", "ParentTitle", "Reason", "ChildCount"])

        self.hubs_log_path = self.log_dir / "hubs.csv"
        self._write_csv_header(self.hubs_log_path, ["Timestamp", "PageTitle", "URL", "ParentTitle", "Reason", "ChildCount"])

    def _write_csv_header(self, filepath: Path, header: List[str]):
        with filepath.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)

    def _append_csv_row(self, filepath: Path, row: List[str]):
        """Appends a row to a CSV file in a thread-safe manner."""
        with self._lock:
            with filepath.open("a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(row)

    def log_other(self, timestamp: str, title: str, url: str, parent: str, reason: str):
        self._append_csv_row(self.other_log_path, [timestamp, title, url, parent, reason])

    def log_collection(self, timestamp: str, title: str, url: str, parent: str, reason: str, children_count: int):
        self._append_csv_row(self.collections_log_path, [timestamp, title, url, parent, reason, str(children_count)])

    def log_hub(self, timestamp: str, title: str, url: str, parent: str, reason: str, children_count: int):
        self._append_csv_row(self.hubs_log_path, [timestamp, title, url, parent, reason, str(children_count)])
        
    def log_debug_html(self, title: str, page_id: int, html_content: str):
        """Sauvegarde le contenu HTML d'une page pour le débogage."""
        try:
            safe_title = _sanitize_filename(title)
            filename = f"hub_id_{page_id}_{safe_title}.html"
            filepath = self.debug_html_dir / filename
            with self._lock:
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(html_content)
            logger.debug(f"HTML for '{title}' saved to '{filepath}' for debugging.")
        except Exception as e:
            logger.error(f"Failed to save debug HTML for page '{title}': {e}")