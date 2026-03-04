import argparse
import asyncio
import logging
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

from .core import ScraperOrchestrator
from .log_manager import LogManager
from .cleaner import main as cleaner_main
from .results_analyzer import main as analyzer_main
from .enricher import PoemEnricher
from .debugger import main as debugger_main

# --- Launch functions for each subcommand ---

def run_scraper(args: argparse.Namespace):
    """Launches the main scraping process."""
    try:
        if getattr(args, "mode", "online") == "offline":
            dumps_dir = getattr(args, "dumps_dir", None)
            if not dumps_dir:
                logging.critical("--dumps-dir is required for offline mode.")
                sys.exit(1)
            if not dumps_dir.exists():
                logging.critical(f"Dumps directory not found: {dumps_dir}")
                sys.exit(1)

            from .offline_core import OfflineOrchestrator
            log_manager = LogManager(args.output_dir / "logs")
            orchestrator = OfflineOrchestrator(config=args, log_manager=log_manager)
            orchestrator.run()
        else:
            log_manager = LogManager(args.output_dir / "logs")
            orchestrator = ScraperOrchestrator(
                config=args,
                log_manager=log_manager,
                bot_username=args.bot_user,
                bot_password=args.bot_pass
            )
            asyncio.run(orchestrator.run())
    except KeyboardInterrupt:
        logging.info("Scraping process interrupted. Shutting down.")
    except Exception as e:
        logging.critical(f"A critical error occurred during scraping: {e}", exc_info=True)
        sys.exit(1)

def run_enricher(args: argparse.Namespace):
    """Launches the data enrichment process."""
    try:
        enricher = PoemEnricher(
            input_path=args.input,
            output_path=args.output,
            lang=args.lang,
            workers=args.workers,
            bot_username=args.bot_user,
            bot_password=args.bot_pass
        )
        asyncio.run(enricher.run())
    except Exception as e:
        logging.critical(f"A critical error occurred during enrichment: {e}", exc_info=True)
        sys.exit(1)

def run_cleaner(args: argparse.Namespace):
    """Launches the cleaning script."""
    try:
        cleaner_argv = ["--input", str(args.input), "--output", str(args.output)]
        return_code = cleaner_main(cleaner_argv)
        if return_code != 0:
            sys.exit(return_code)
    except Exception as e:
        logging.critical(f"A critical error occurred during cleaning: {e}", exc_info=True)
        sys.exit(1)

def run_analyzer(args: argparse.Namespace):
    """Launches the analysis script."""
    try:
        analyzer_argv = [str(args.filepath)] if args.filepath else []
        analyzer_main(analyzer_argv)
    except Exception as e:
        logging.critical(f"A critical error occurred during analysis: {e}", exc_info=True)
        sys.exit(1)

def run_debugger(args: argparse.Namespace):
    """Launches the debug script to extract unidentified collections."""
    try:
        debugger_argv = ["--input", str(args.input), "--output", str(args.output)]
        return_code = debugger_main(debugger_argv)
        if return_code != 0:
            sys.exit(return_code)
    except Exception as e:
        logging.critical(f"A critical error occurred during debugging: {e}", exc_info=True)
        sys.exit(1)

def main_cli():
    """Main entry point of the command-line interface."""
    parser = argparse.ArgumentParser(
        description="Scriptorium v5 - A comprehensive tool to scrape (online/offline), clean, and analyze poems.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging (DEBUG)."
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # --- 'scrape' command ---
    p_scrape = subparsers.add_parser("scrape", help="Launch a new Wikisource scraping run.")
    p_scrape.add_argument("--lang", type=str, required=True, help="Language code (e.g., 'fr', 'en').")
    p_scrape.add_argument("--category", type=str, required=True, help="Root category (e.g., 'Poèmes par Auteur').")
    p_scrape.add_argument("--output_dir", type=Path, default=Path("./data"), help="Output directory (default: ./data/).")
    p_scrape.add_argument("--workers", type=int, default=3, help="Number of parallel requests (default: 3).")
    p_scrape.add_argument("--limit", type=int, default=None, help="Limit the number of pages to process (for testing).")
    p_scrape.add_argument("--resume", action="store_true", help="Resume an interrupted scraping run.")
    p_scrape.add_argument("--tree-log", action="store_true", help="Generate tree-structured exploration logs.")
    p_scrape.add_argument(
        "--mode", type=str, choices=["online", "offline"], default="online",
        help="Scraping mode: 'online' (API, default) or 'offline' (local dumps)."
    )
    p_scrape.add_argument(
        "--dumps-dir", type=Path, default=None,
        help="Directory containing Wikimedia dump files (required for offline mode)."
    )
    p_scrape.set_defaults(func=run_scraper)

    # --- 'enrich' command ---
    p_enrich = subparsers.add_parser("enrich", help="Enrich a data file with missing collection_page_ids.")
    p_enrich.add_argument("--input", "-i", type=Path, required=True, help="Input file (e.g., data/poems.cleaned.jsonl.gz).")
    p_enrich.add_argument("--output", "-o", type=Path, required=True, help="Enriched output file (e.g., data/poems.enriched.jsonl.gz).")
    p_enrich.add_argument("--lang", type=str, required=True, help="Language code of the corresponding Wikisource project.")
    p_enrich.add_argument("--workers", type=int, default=3, help="Number of parallel API requests (default: 3).")
    p_enrich.set_defaults(func=run_enricher)

    # --- 'clean' command ---
    p_clean = subparsers.add_parser("clean", help="Clean and deduplicate a results file.")
    p_clean.add_argument("--input", "-i", type=Path, required=True, help="Input file (e.g., data/poems.jsonl.gz).")
    p_clean.add_argument("--output", "-o", type=Path, required=True, help="Cleaned output file (e.g., data/poems.cleaned.jsonl.gz).")
    p_clean.set_defaults(func=run_cleaner)

    # --- 'analyze' command ---
    p_analyze = subparsers.add_parser("analyze", help="Analyze a data file and display statistics.")
    p_analyze.add_argument("filepath", type=Path, nargs='?', default=None, help="Path to the file to analyze (optional, searches in data/ by default).")
    p_analyze.set_defaults(func=run_analyzer)

    # --- 'debug' command ---
    p_debug = subparsers.add_parser("debug", help="Extract poems from unidentified collections for analysis.")
    p_debug.add_argument("--input", "-i", type=Path, required=True, help="Input file to analyze (e.g., data/poems.enriched.jsonl.gz).")
    p_debug.add_argument("--output", "-o", type=Path, required=True, help="Output file for extracted poems (e.g., data/debug.unidentified.jsonl.gz).")
    p_debug.set_defaults(func=run_debugger)

    # Optional global variables for bot authentication
    parser.add_argument("--bot-user", type=str, default=os.getenv("WIKISOURCE_BOT_USERNAME"), help="Bot username (or WIKISOURCE_BOT_USERNAME environment variable)")
    parser.add_argument("--bot-pass", type=str, default=os.getenv("WIKISOURCE_BOT_PASSWORD"), help="Bot password (or WIKISOURCE_BOT_PASSWORD environment variable)")

    args = parser.parse_args()

    # Logging configuration
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", stream=sys.stdout)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)

    # Execute the function associated with the command
    args.func(args)

if __name__ == "__main__":
    main_cli()
