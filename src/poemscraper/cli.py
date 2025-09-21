import argparse
import asyncio
import logging
import sys
from pathlib import Path

from .core import ScraperOrchestrator
from .log_manager import LogManager

def main_cli():
    """Parses arguments and launches the main scraping process."""
    parser = argparse.ArgumentParser(
        description="A robust, asynchronous scraper for extracting structured poems from Wikisource."
    )
    parser.add_argument(
        "--lang",
        type=str,
        required=True,
        help="Language code of the Wikisource to target (e.g., 'fr', 'en', 'de')."
    )
    parser.add_argument(
        "--category",
        type=str,
        required=True,
        help="The root category to start crawling from (e.g., 'Poèmes', 'Poetry')."
    )
    parser.add_argument(
        "--output_dir",
        type=Path,
        default=Path("./data"),
        help="Directory to store the output files (NDJSON and SQLite index). Default: ./data/"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of concurrent API request workers. Default: 10."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the total number of poems to scrape (useful for testing)."
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Enable resume mode. Skips page_ids already present in the SQLite index."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Performs a dry run: identifies pages to be scraped but does not fetch or process them."
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose (DEBUG) logging."
    )
    
    parser.add_argument(
        "--tree-log",
        action="store_true",
        help="Generate detailed exploration tree logs for each author in the output 'logs' directory."
    )
    
    parser.add_argument(
        "--cleaned",
        type=str,
        default="true",
        choices=["true", "false"],
        help="Write an additional cleaned JSONL.GZ (true|false). Default: true."
    )
    
    args = parser.parse_args()
    
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )

    log_manager = LogManager(args.output_dir / "logs")

    try:
        orchestrator = ScraperOrchestrator(config=args, log_manager=log_manager)
        asyncio.run(orchestrator.run())
    except KeyboardInterrupt:
        logging.info("Scraping process interrupted by user. Exiting gracefully.")
        sys.exit(0)
    except Exception as e:
        logging.critical(f"A critical error occurred: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main_cli()