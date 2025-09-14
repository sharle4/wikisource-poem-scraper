from __future__ import annotations

import gzip
import io
import json
import sys
from pathlib import Path
from collections import Counter
from typing import Iterator, Dict, Any

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
        self.total_poems = 0
        self.poems_with_short_text = 0
        
        self.completeness_counters: Counter[str] = Counter()
        
        self.unique_authors: set[str] = set()
        self.unique_collections: set[str] = set()
        self.unique_hubs: set[int] = set()
        self.author_counts: Counter[str] = Counter()
        self.collection_counts: Counter[str] = Counter()
        self.hub_counts: Counter[int] = Counter()
        
        self.checksum_counts: Counter[str] = Counter()
        self.title_counts: Counter[str] = Counter()
        self.page_id_counts: Counter[int] = Counter()

        self.real_hubs: set[int] = set()

    def analyze(self):
        """Lance le processus d'analyse et affiche le rapport final."""
        print(f"[*] Début de l'analyse de {self.filepath}...")
        
        for poem in iter_jsonl(self.filepath):
            self._process_poem(poem)

        self._post_process_analysis()
        
        print("\n[*] Analyse terminée. Génération du rapport...")
        self._print_report()

    def _process_poem(self, poem: dict[str, Any]):
        """Traite un seul poème et met à jour les statistiques."""
        self.total_poems += 1
        
        metadata = poem.get("metadata", {})
        
        if metadata.get("author"): self.completeness_counters["author"] += 1
        if metadata.get("source_collection"): self.completeness_counters["collection"] += 1
        if metadata.get("publication_date"): self.completeness_counters["date"] += 1
        if metadata.get("publisher"): self.completeness_counters["publisher"] += 1
        if metadata.get("translator"): self.completeness_counters["translator"] += 1
        if metadata.get("license_name"): self.completeness_counters["license"] += 1
        if poem.get("hub_title"): self.completeness_counters["in_hub"] += 1
            
        author = metadata.get("author")
        if author:
            self.unique_authors.add(author)
            self.author_counts[author] += 1
            
        collection = metadata.get("source_collection")
        if collection:
            self.unique_collections.add(collection)
            self.collection_counts[collection] += 1

        hub_id = poem.get("hub_page_id")
        if hub_id is not None:
            self.unique_hubs.add(hub_id)
            self.hub_counts[hub_id] += 1
            
        if poem.get("checksum_sha256"): self.checksum_counts[poem["checksum_sha256"]] += 1
        if poem.get("title"): self.title_counts[poem["title"]] += 1
        if poem.get("page_id"): self.page_id_counts[poem["page_id"]] += 1
            
        if len(poem.get("normalized_text", "")) < 20:
            self.poems_with_short_text += 1

    def _post_process_analysis(self):
        """Effectue des calculs finaux après avoir parcouru tout le fichier."""
        for hub_id, count in self.hub_counts.items():
            if count > 1:
                self.real_hubs.add(hub_id)
        self.completeness_counters["in_hub"] = len(self.real_hubs)
                
    def _print_report(self):
        """Affiche le rapport statistique final de manière structurée."""
        print("\n" + "="*80)
        print(" " * 25 + "ANALYSE DU CORPUS DE POÈMES")
        print("="*80)

        print("\n--- Statistiques Globales ---\n")
        print(f"{'Total des poèmes analysés:':<45} {self.total_poems}")
        print(f"{'Nombre d\'auteurs uniques:':<45} {len(self.unique_authors)}")
        print(f"{'Nombre de recueils uniques:':<45} {len(self.unique_collections)}")
        print(f"{'Nombre de groupes de poèmes (hubs):':<45} {len(self.unique_hubs)}")
        print(f"{'  ↳ Hubs multi-versions réels (>1 poème):':<45} {len(self.real_hubs)}")
        print(f"{'  ↳ Poèmes autonomes (1 poème par groupe):':<45} {len(self.unique_hubs) - len(self.real_hubs)}")

        print("\n--- Qualité et Complétude des Données ---\n")
        
        def print_completeness(key: str, label: str):
            count = self.completeness_counters[key]
            percentage = (count / self.total_poems * 100) if self.total_poems else 0
            print(f"{label:<45} {count} ({percentage:.2f}%)")

        print_completeness("author", "Poèmes avec un auteur identifié:")
        print_completeness("collection", "Poèmes avec un recueil source identifié:")
        print_completeness("in_hub", "Poèmes appartenant à un hub multi-versions:")
        print_completeness("date", "Poèmes avec une date de publication:")
        print_completeness("publisher", "Poèmes avec un éditeur identifié:")
        print_completeness("translator", "Poèmes avec un traducteur identifié:")
        print_completeness("license", "Poèmes avec une licence identifiée:")
        
        print("\n--- Problèmes Potentiels Détectés ---\n")
        print(f"{'Poèmes avec texte très court (<20 caractères):':<45} {self.poems_with_short_text}")

        print("\n--- Analyse des Doublons ---\n")
        
        def print_duplicates(counter: Counter, label: str, note: str = ""):
            duplicates = sum(count - 1 for count in counter.values() if count > 1)
            items_with_duplicates = len([c for c in counter.values() if c > 1])
            print(f"{label:<45}")
            if note: print(f"  ({note})")
            print(f"{'  ↳ Nombre total d\'entrées dupliquées:':<45} {duplicates}")
            print(f"{'  ↳ Nombre d\'items uniques ayant des doublons:':<45} {items_with_duplicates}")

        print_duplicates(self.checksum_counts, "Basé sur le checksum du wikitext", "Contenu brut strictement identique")
        print_duplicates(self.title_counts, "Basé sur le titre du poème (nettoyé)")
        print_duplicates(self.page_id_counts, "Basé sur le page_id", "Devrait être 0. Indique une erreur dans le scraping si > 0")

        print("\n--- Classements (Top 10) ---\n")
        print("Auteurs les plus prolifiques :")
        for author, count in self.author_counts.most_common(10):
            print(f"  - {author}: {count} poèmes")

        print("\nRecueils les plus fournis :")
        for collection, count in self.collection_counts.most_common(10):
            print(f"  - {collection}: {count} poèmes")

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
    except Exception:
        repo_root = None

    candidates = [
        Path("data/poems.cleaned.jsonl.gz"),
        Path("data/poems.jsonl.gz"),
    ]
    if repo_root is not None:
        candidates.extend([
            repo_root / "data" / "poems.cleaned.jsonl.gz",
            repo_root / "data" / "poems.jsonl.gz",
        ])

    target = next((p for p in candidates if p.exists()), None)
    if target is None:
        print("[ERREUR] Aucun fichier trouvé parmi les candidats:", file=sys.stderr)
        for p in candidates:
            print(f"  - {p}", file=sys.stderr)
        sys.exit(1)

    analyzer = ResultsAnalyzer(target)
    analyzer.analyze()

if __name__ == "__main__":
    main()