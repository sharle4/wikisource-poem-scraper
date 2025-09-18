from __future__ import annotations

import gzip
import io
import json
import sys
from pathlib import Path
from collections import Counter, defaultdict
from typing import Iterator, Dict, Any, Set

def is_gz(path: Path) -> bool:
    """Vérifie si un fichier est compressé avec Gzip."""
    return path.suffix == ".gz" or path.name.endswith(".jsonl.gz")

def open_maybe_gzip(path: Path, mode: str):
    """Ouvre un fichier, en gérant la décompression Gzip de manière transparente."""
    if "b" in mode:
        return gzip.open(path, mode) if is_gz(path) else open(path, mode)
    if is_gz(path):
        gz = gzip.open(path, mode.replace("t", "b"))
        return io.TextIOWrapper(gz, encoding="utf-8")
    return open(path, mode, encoding="utf-8")

def iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    """Itère sur les lignes d'un fichier JSONL, en gérant les erreurs de parsing."""
    with open_maybe_gzip(path, "rt") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                print(f"[ERREUR] Erreur de décodage JSON à la ligne {line_num}", file=sys.stderr)
                continue

class ResultsAnalyzer:
    """Orchestre l'analyse complète du fichier de résultats."""

    def __init__(self, filepath: Path):
        self.filepath = filepath
        # Statistiques générales
        self.total_poems = 0
        self.poems_with_author = 0
        self.poems_with_date = 0
        self.poems_with_short_text = 0
        self.author_counts: Counter[str] = Counter()
        self.checksum_counts: Counter[str] = Counter()

        # Nouvelles statistiques pour les recueils
        self.poems_in_collection = 0
        self.poems_with_section = 0
        self.collection_poem_counts: Counter[str] = Counter()
        self.collection_section_data: Dict[str, Set[str]] = defaultdict(set)
        self.unique_collection_ids: Set[int] = set()

        # Statistiques pour les hubs
        self.hub_counts: Counter[int] = Counter()
        self.real_hubs: Set[int] = set()

    def analyze(self):
        """Lance le processus d'analyse et affiche le rapport final."""
        print(f"[*] Début de l'analyse de {self.filepath}...")
        
        for poem in iter_jsonl(self.filepath):
            self._process_poem(poem)

        self._post_process_analysis()
        
        print("[*] Analyse terminée. Génération du rapport...")
        self._print_report()

    def _process_poem(self, poem: dict[str, Any]):
        """Traite un seul poème et met à jour les statistiques."""
        self.total_poems += 1
        
        # Métadonnées de base
        metadata = poem.get("metadata", {})
        author = metadata.get("author")
        if author:
            self.poems_with_author += 1
            self.author_counts[author] += 1
            
        if metadata.get("publication_date"):
            self.poems_with_date += 1

        # Analyse de la structure des recueils
        collection_id = poem.get("collection_page_id")
        collection_title = poem.get("collection_title")
        if collection_id and collection_title:
            self.poems_in_collection += 1
            self.unique_collection_ids.add(collection_id)
            self.collection_poem_counts[collection_title] += 1
            
            section_title = poem.get("section_title")
            if section_title:
                self.poems_with_section += 1
                self.collection_section_data[collection_title].add(section_title)

        # Analyse des hubs
        hub_id = poem.get("hub_page_id")
        if hub_id is not None:
            self.hub_counts[hub_id] += 1
            
        # Données techniques
        checksum = poem.get("checksum_sha256")
        if checksum:
            self.checksum_counts[checksum] += 1
        if len(poem.get("normalized_text", "")) < 20:
            self.poems_with_short_text += 1

    def _post_process_analysis(self):
        """Effectue des calculs finaux après avoir parcouru tout le fichier."""
        for hub_id, count in self.hub_counts.items():
            if count > 1:
                self.real_hubs.add(hub_id)
                
    def _print_report(self):
        """Affiche le rapport statistique final de manière structurée."""
        print("\n" + "="*80)
        print(" " * 25 + "ANALYSE DU CORPUS DE POÈMES")
        print("="*80)

        # --- Section 1: Statistiques Globales ---
        print("\n--- Statistiques Globales ---\n")
        print(f"{'Total des poèmes analysés:':<45} {self.total_poems}")
        print(f"{'Nombre d\'auteurs uniques:':<45} {len(self.author_counts)}")
        
        # --- Section 2: Analyse des Recueils ---
        print("\n--- Analyse des Recueils ---\n")
        collection_completeness = (self.poems_in_collection / self.total_poems * 100) if self.total_poems else 0
        section_completeness = (self.poems_with_section / self.total_poems * 100) if self.total_poems else 0
        print(f"{'Nombre de recueils uniques:':<45} {len(self.unique_collection_ids)}")
        print(f"{'Poèmes appartenant à un recueil:':<45} {self.poems_in_collection} ({collection_completeness:.2f}%)")
        print(f"{'Poèmes avec un titre de section:':<45} {self.poems_with_section} ({section_completeness:.2f}%)")
        
        # --- Section 3: Analyse des Hubs ---
        print("\n--- Analyse des Hubs (Multi-versions) ---\n")
        print(f"{'Nombre de hubs uniques (groupes de poèmes):':<45} {len(self.hub_counts)}")
        print(f"{'  ↳ Hubs multi-versions réels (>1 poème):':<45} {len(self.real_hubs)}")
        print(f"{'  ↳ Poèmes autonomes (agissant comme leur propre hub):':<45} {len(self.hub_counts) - len(self.real_hubs)}")

        # --- Section 4: Qualité et Complétude ---
        print("\n--- Qualité et Complétude des Données ---\n")
        author_completeness = (self.poems_with_author / self.total_poems * 100) if self.total_poems else 0
        date_completeness = (self.poems_with_date / self.total_poems * 100) if self.total_poems else 0
        print(f"{'Poèmes avec un auteur identifié:':<45} {self.poems_with_author} ({author_completeness:.2f}%)")
        print(f"{'Poèmes avec une date de publication:':<45} {self.poems_with_date} ({date_completeness:.2f}%)")
        
        # --- Section 5: Problèmes Potentiels ---
        print("\n--- Problèmes Potentiels Détectés ---\n")
        exact_duplicates = sum(count - 1 for count in self.checksum_counts.values() if count > 1)
        print(f"{'Poèmes avec texte très court (<20 caractères):':<45} {self.poems_with_short_text}")
        print(f"{'Poèmes en double (contenu identique):':<45} {exact_duplicates}")

        # --- Section 6: Classements (Top 10) ---
        print("\n--- Classements (Top 10) ---\n")
        print("Auteurs les plus prolifiques :")
        for author, count in self.author_counts.most_common(10):
            print(f"  - {author}: {count} poèmes")

        print("\nRecueils les plus fournis :")
        for collection, count in self.collection_poem_counts.most_common(10):
            print(f"  - {collection}: {count} poèmes")
            
        print("\nRecueils les plus structurés (par nb de sections) :")
        collections_by_section_count = sorted(
            self.collection_section_data.items(),
            key=lambda item: len(item[1]),
            reverse=True
        )
        for collection, sections in collections_by_section_count[:10]:
            print(f"  - {collection}: {len(sections)} sections")

        print("\nHubs avec le plus de versions :")
        real_hub_counts = {k: v for k, v in self.hub_counts.items() if v > 1}
        top_hubs = Counter(real_hub_counts).most_common(10)
        
        if top_hubs:
            print("  (Note: l'identifiant de la page hub est affiché)")
            for hub_id, count in top_hubs:
                print(f"  - Hub ID {hub_id}: {count} versions")
        else:
            print("  Aucun hub multi-versions trouvé dans le corpus.")
            
        print("\n" + "="*80)

def main():
    """Point d'entrée: cherche automatiquement un fichier à analyser dans data/."""
    try:
        repo_root = Path(__file__).resolve().parents[2]
    except IndexError:
        repo_root = Path.cwd()

    candidates = [
        repo_root / "data" / "poems.cleaned.jsonl.gz",
        repo_root / "data" / "poems.jsonl.gz",
    ]

    target = next((p for p in candidates if p.exists()), None)
    if target is None:
        print("[ERREUR] Aucun fichier de résultats (poems.jsonl.gz ou poems.cleaned.jsonl.gz) trouvé dans le répertoire 'data/'.", file=sys.stderr)
        print("Veuillez lancer le scraper d'abord.", file=sys.stderr)
        sys.exit(1)

    analyzer = ResultsAnalyzer(target)
    analyzer.analyze()

if __name__ == "__main__":
    main()