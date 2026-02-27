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

# --- Fonctions de lancement pour chaque sous-commande ---

def run_scraper(args: argparse.Namespace):
    """Lance le processus de scraping principal."""
    try:
        log_manager = LogManager(args.output_dir / "logs")
        orchestrator = ScraperOrchestrator(
            config=args, 
            log_manager=log_manager,
            bot_username=args.bot_user,
            bot_password=args.bot_pass
        )
        asyncio.run(orchestrator.run())
    except KeyboardInterrupt:
        logging.info("Processus de scraping interrompu. Arrêt en cours.")
    except Exception as e:
        logging.critical(f"Une erreur critique est survenue durant le scraping : {e}", exc_info=True)
        sys.exit(1)

def run_enricher(args: argparse.Namespace):
    """Lance le processus d'enrichissement des données."""
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
        logging.critical(f"Une erreur critique est survenue durant l'enrichissement : {e}", exc_info=True)
        sys.exit(1)

def run_cleaner(args: argparse.Namespace):
    """Lance le script de nettoyage."""
    try:
        cleaner_argv = ["--input", str(args.input), "--output", str(args.output)]
        return_code = cleaner_main(cleaner_argv)
        if return_code != 0:
            sys.exit(return_code)
    except Exception as e:
        logging.critical(f"Une erreur critique est survenue durant le nettoyage : {e}", exc_info=True)
        sys.exit(1)

def run_analyzer(args: argparse.Namespace):
    """Lance le script d'analyse."""
    try:
        analyzer_argv = [str(args.filepath)] if args.filepath else []
        analyzer_main(analyzer_argv)
    except Exception as e:
        logging.critical(f"Une erreur critique est survenue durant l'analyse : {e}", exc_info=True)
        sys.exit(1)

def run_debugger(args: argparse.Namespace):
    """Lance le script de débogage pour extraire les recueils non identifiés."""
    try:
        debugger_argv = ["--input", str(args.input), "--output", str(args.output)]
        return_code = debugger_main(debugger_argv)
        if return_code != 0:
            sys.exit(return_code)
    except Exception as e:
        logging.critical(f"Une erreur critique est survenue durant le débogage : {e}", exc_info=True)
        sys.exit(1)

def main_cli():
    """Point d'entrée principal de l'interface en ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Wikisource Poem Scraper v4 - Un outil complet pour scraper, nettoyer et analyser des poèmes.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Activer les logs détaillés (DEBUG)."
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="Commandes disponibles")

    # --- Commande 'scrape' ---
    p_scrape = subparsers.add_parser("scrape", help="Lancer un nouveau scraping de Wikisource.")
    p_scrape.add_argument("--lang", type=str, required=True, help="Code langue (ex: 'fr', 'en').")
    p_scrape.add_argument("--category", type=str, required=True, help="Catégorie racine (ex: 'Poèmes par Auteur').")
    p_scrape.add_argument("--output_dir", type=Path, default=Path("./data"), help="Répertoire de sortie (défaut: ./data/).")
    p_scrape.add_argument("--workers", type=int, default=10, help="Nombre de requêtes parallèles (défaut: 10).")
    p_scrape.add_argument("--limit", type=int, default=None, help="Limite le nombre de pages à traiter (pour test).")
    p_scrape.add_argument("--resume", action="store_true", help="Reprendre un scraping interrompu.")
    p_scrape.add_argument("--tree-log", action="store_true", help="Générer des logs d'exploration en arborescence.")
    p_scrape.set_defaults(func=run_scraper)

    # --- Commande 'enrich' ---
    p_enrich = subparsers.add_parser("enrich", help="Enrichir un fichier de données avec les collection_page_id manquants.")
    p_enrich.add_argument("--input", "-i", type=Path, required=True, help="Fichier d'entrée (ex: data/poems.cleaned.jsonl.gz).")
    p_enrich.add_argument("--output", "-o", type=Path, required=True, help="Fichier de sortie enrichi (ex: data/poems.enriched.jsonl.gz).")
    p_enrich.add_argument("--lang", type=str, required=True, help="Code langue du projet Wikisource correspondant aux données.")
    p_enrich.add_argument("--workers", type=int, default=20, help="Nombre de requêtes API parallèles (défaut: 20).")
    p_enrich.set_defaults(func=run_enricher)
    
    # --- Commande 'clean' ---
    p_clean = subparsers.add_parser("clean", help="Nettoyer et dédupliquer un fichier de résultats.")
    p_clean.add_argument("--input", "-i", type=Path, required=True, help="Fichier d'entrée (ex: data/poems.jsonl.gz).")
    p_clean.add_argument("--output", "-o", type=Path, required=True, help="Fichier de sortie nettoyé (ex: data/poems.cleaned.jsonl.gz).")
    p_clean.set_defaults(func=run_cleaner)
    
    # --- Commande 'analyze' ---
    p_analyze = subparsers.add_parser("analyze", help="Analyser un fichier de données et afficher des statistiques.")
    p_analyze.add_argument("filepath", type=Path, nargs='?', default=None, help="Chemin du fichier à analyser (optionnel, cherche dans data/ par défaut).")
    p_analyze.set_defaults(func=run_analyzer)

    # --- Commande 'debug' ---
    p_debug = subparsers.add_parser("debug", help="Extraire les poèmes de recueils non identifiés pour analyse.")
    p_debug.add_argument("--input", "-i", type=Path, required=True, help="Fichier d'entrée à analyser (ex: data/poems.enriched.jsonl.gz).")
    p_debug.add_argument("--output", "-o", type=Path, required=True, help="Fichier de sortie pour les poèmes extraits (ex: data/debug.unidentified.jsonl.gz).")
    p_debug.set_defaults(func=run_debugger)

    # Variables globales optionnelles pour l'authentification (bot)
    parser.add_argument("--bot-user", type=str, default=os.getenv("WIKISOURCE_BOT_USERNAME"), help="Nom d'utilisateur du bot (ou variable d'environnement WIKISOURCE_BOT_USERNAME)")
    parser.add_argument("--bot-pass", type=str, default=os.getenv("WIKISOURCE_BOT_PASSWORD"), help="Mot de passe du bot (ou variable d'environnement WIKISOURCE_BOT_PASSWORD)")

    args = parser.parse_args()

    # Configuration du logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", stream=sys.stdout)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)

    # Exécution de la fonction associée à la commande
    args.func(args)

if __name__ == "__main__":
    main_cli()