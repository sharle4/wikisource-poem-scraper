import logging
import csv
from pathlib import Path
from threading import Lock
from typing import List

class LogManager:
    """Centralizes the management of all logging for the application."""

    def __init__(self, log_dir: Path):
        self.log_dir = log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()

        self.file_handler = logging.FileHandler(self.log_dir / "scraper.log", mode='w', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.file_handler.setFormatter(formatter)
        logging.getLogger().addHandler(self.file_handler)

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