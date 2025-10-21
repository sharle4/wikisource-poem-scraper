"""
Module d'enrichissement des données pour corriger les `collection_page_id` manquants.

Ce script exécute un processus en trois étapes pour améliorer un fichier de données existant :
1.  **Analyse et mise en cache** : Il lit une première fois le fichier d'entrée pour
    construire un cache des correspondances `collection_title` -> `collection_page_id`
    déjà connues et identifie tous les titres de recueils qui nécessitent une recherche d'ID.
2.  **Récupération API** : Pour tous les titres sans ID, il interroge l'API MediaWiki
    de manière asynchrone et massive pour trouver les `pageid` correspondants,
    en gérant les redirections.
3.  **Enrichissement et écriture** : Il relit le fichier d'entrée et écrit un nouveau
    fichier de sortie, en ajoutant les `collection_page_id` qui ont été trouvés.
"""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Dict, Set, List, Any

from tqdm import tqdm

from .api_client import WikiAPIClient
from .utils import iter_jsonl, open_maybe_gzip

logger = logging.getLogger(__name__)


class PoemEnricher:
    """Orchestre le processus d'enrichissement des données de poèmes."""

    def __init__(self, input_path: Path, output_path: Path, lang: str, workers: int):
        self.input_path = input_path
        self.output_path = output_path
        self.lang = lang
        self.workers = workers
        self.api_endpoint = f"https://{lang}.wikisource.org/w/api.php"
        self.title_to_id_cache: Dict[str, int] = {}

    async def run(self):
        """Exécute le workflow complet d'enrichissement."""
        logger.info(f"Début du processus d'enrichissement pour '{self.input_path}'.")

        if not self.input_path.exists():
            logger.critical(f"Le fichier d'entrée '{self.input_path}' est introuvable.")
            return

        # --- Étape 1: Construire le cache initial et identifier les titres manquants ---
        titles_to_fetch = await self._build_initial_cache_and_identify_missing()

        # --- Étape 2: Récupérer les IDs manquants via l'API ---
        if titles_to_fetch:
            logger.info(f"Récupération de {len(titles_to_fetch)} IDs de recueils manquants via l'API...")
            async with WikiAPIClient(self.api_endpoint, self.workers) as client:
                await self._fetch_missing_ids_from_api(client, list(titles_to_fetch))
        else:
            logger.info("Aucun ID de recueil manquant à récupérer. Le cache est complet.")

        # --- Étape 3: Enrichir le fichier d'origine et écrire le nouveau ---
        await self._enrich_and_write_file()

        logger.info(f"Processus terminé. Fichier enrichi sauvegardé dans '{self.output_path}'.")

    async def _build_initial_cache_and_identify_missing(self) -> Set[str]:
        """
        Lit le fichier une fois pour créer un cache des IDs connus et lister les titres à chercher.
        Ceci est une optimisation pour éviter des appels API inutiles.
        """
        logger.info("Phase 1: Analyse du fichier pour construire le cache initial...")
        titles_needing_id = set()
        
        total_lines = sum(1 for _ in open_maybe_gzip(self.input_path, "rt"))

        with tqdm(total=total_lines, desc="Analyse des poèmes", unit=" poème") as pbar:
            for poem in iter_jsonl(self.input_path):
                collection_title = poem.get("collection_title")
                collection_id = poem.get("collection_page_id")

                if collection_title:
                    if collection_id is not None:
                        self.title_to_id_cache.setdefault(collection_title, collection_id)
                    else:
                        titles_needing_id.add(collection_title)
                pbar.update(1)
        
        titles_to_fetch = titles_needing_id - set(self.title_to_id_cache.keys())
        logger.info(f"Analyse terminée. {len(self.title_to_id_cache)} IDs trouvés dans le cache. "
                    f"{len(titles_to_fetch)} IDs uniques à récupérer.")
        return titles_to_fetch

    async def _fetch_missing_ids_from_api(self, client: WikiAPIClient, titles: List[str]):
        """
        Interroge l'API MediaWiki par lots pour trouver les IDs des titres de recueils.
        """
        batch_size = 50
        tasks = []
        for i in range(0, len(titles), batch_size):
            batch = titles[i:i + batch_size]
            tasks.append(client.get_page_info_and_redirects(batch))

        found_count = 0
        with tqdm(total=len(tasks), desc="Appels API", unit=" lot") as pbar:
            for future in asyncio.as_completed(tasks):
                try:
                    query_result = await future
                    if query_result:
                        self._process_api_result(query_result)
                        found_count += len(query_result.get("pages", []))
                except Exception as e:
                    logger.error(f"Un lot d'appels API a échoué : {e}", exc_info=True)
                pbar.update(1)
        
        logger.info(f"{len(self.title_to_id_cache) - found_count} IDs ont été ajoutés au cache via l'API.")

    def _process_api_result(self, query_result: Dict[str, Any]):
        """Traite le résultat d'un appel API pour mettre à jour le cache `title_to_id`."""
        pages = {p['title']: p for p in query_result.get("pages", []) if "missing" not in p}
        redirects = {r['from']: r['to'] for r in query_result.get("redirects", [])}

        for title, page_info in pages.items():
            page_id = page_info.get("pageid")
            if page_id:
                self.title_to_id_cache[title] = page_id
        
        for from_title, to_title in redirects.items():
            if to_title in self.title_to_id_cache:
                self.title_to_id_cache[from_title] = self.title_to_id_cache[to_title]

    async def _enrich_and_write_file(self):
        """Lit le fichier d'entrée une seconde fois, enrichit les données et écrit le fichier de sortie."""
        logger.info("Phase 2: Enrichissement et écriture du nouveau fichier...")
        enriched_count = 0
        total_lines = sum(1 for _ in open_maybe_gzip(self.input_path, "rt"))

        with open_maybe_gzip(self.output_path, "wt") as fout:
            with tqdm(total=total_lines, desc="Écriture des poèmes", unit=" poème") as pbar:
                for poem in iter_jsonl(self.input_path):
                    if poem.get("collection_page_id") is None:
                        title = poem.get("collection_title")
                        if title and title in self.title_to_id_cache:
                            poem["collection_page_id"] = self.title_to_id_cache[title]
                            enriched_count += 1
                    
                    fout.write(json.dumps(poem, ensure_ascii=False) + "\n")
                    pbar.update(1)
        
        logger.info(f"Écriture terminée. {enriched_count} poèmes ont été enrichis avec un `collection_page_id`.")
