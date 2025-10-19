from __future__ import annotations

import gzip
import io
import json
import sys
import statistics
from pathlib import Path
from collections import Counter, defaultdict
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

class CorpusAnalyzer:
    """
    Orchestre une analyse complète et détaillée du corpus de poèmes,
    en extrayant des statistiques sur la structure, le contenu et les métadonnées,
    et en distinguant rigoureusement les données structurées des données inférées.
    """

    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.total_poems = 0

        # Compteurs de complétude des métadonnées
        self.poems_with_author = 0
        self.poems_with_date = 0
        self.poems_with_publisher = 0
        self.poems_with_translator = 0
        
        # Compteurs structurels
        self.poems_with_identified_collection = 0
        self.poems_with_unidentified_collection = 0
        self.poems_with_section = 0
        self.poems_with_order = 0

        # Données pour l'analyse de contenu
        self.total_stanzas = 0
        self.total_verses = 0
        self.poem_lengths_data: list[dict] = []

        # Structures pour l'analyse des entités
        self.authors_data = defaultdict(lambda: {"poem_count": 0, "collection_ids": set()})
        self.collections_by_id: Dict[int, Dict[str, Any]] = defaultdict(
            lambda: {"poem_count": 0, "titles": set(), "sections": set(), "authors": set()}
        )
        self.collections_by_title_only: Counter[str] = Counter()
        self.hubs_data = defaultdict(lambda: {"version_count": 0, "title": ""})
        self.checksum_counts: Counter[str] = Counter()

    def analyze_and_report(self):
        """Lance le processus d'analyse et affiche le rapport final."""
        print(f"[*] Début de l'analyse détaillée de {self.filepath}...")

        for poem in iter_jsonl(self.filepath):
            self._process_poem(poem)

        print("[*] Analyse terminée. Génération du rapport exhaustif...")
        self._print_report()

    def _process_poem(self, poem: dict[str, Any]):
        """Traite un seul poème et met à jour toutes les métriques statistiques."""
        self.total_poems += 1
        
        # --- Analyse des Métadonnées ---
        metadata = poem.get("metadata", {})
        author = metadata.get("author")
        if author:
            self.poems_with_author += 1
            self.authors_data[author]["poem_count"] += 1
        
        if metadata.get("publication_date"): self.poems_with_date += 1
        if metadata.get("publisher"): self.poems_with_publisher += 1
        if metadata.get("translator"): self.poems_with_translator += 1

        # --- Analyse Structurelle (Recueils et Sections) ---
        collection_page_id = poem.get("collection_page_id")
        collection_title = poem.get("collection_title")

        if collection_page_id:
            self.poems_with_identified_collection += 1
            collection_entry = self.collections_by_id[collection_page_id]
            collection_entry["poem_count"] += 1
            if collection_title:
                collection_entry["titles"].add(collection_title)
            if author:
                collection_entry["authors"].add(author)
                self.authors_data[author]["collection_ids"].add(collection_page_id)
            
            section_title = poem.get("section_title")
            if section_title:
                self.poems_with_section += 1
                collection_entry["sections"].add(section_title)
        
        elif collection_title:
            self.poems_with_unidentified_collection += 1
            self.collections_by_title_only[collection_title] += 1

        if poem.get("poem_order") is not None:
            self.poems_with_order += 1

        # --- Analyse des Hubs (Multi-versions) ---
        hub_id = poem.get("hub_page_id")
        if hub_id is not None:
            self.hubs_data[hub_id]["version_count"] += 1
            if not self.hubs_data[hub_id]["title"]:
                self.hubs_data[hub_id]["title"] = poem.get("hub_title") or f"Poème autonome: {poem.get('title', 'N/A')}"

        # --- Analyse du Contenu ---
        structure = poem.get("structure", {})
        stanzas = structure.get("stanzas", [])
        num_verses = sum(len(s) for s in stanzas)
        
        self.total_stanzas += len(stanzas)
        self.total_verses += num_verses
        
        self.poem_lengths_data.append({
            "verses": num_verses,
            "title": poem.get("title", "Titre inconnu"),
            "author": metadata.get("author", "Auteur inconnu")
        })
        
        # --- Analyse Technique ---
        checksum = poem.get("checksum_sha256")
        if checksum: self.checksum_counts[checksum] += 1
    
    def _print_report(self):
        """Affiche le rapport statistique final de manière structurée et professionnelle."""
        
        def print_header(title):
            print("\n" + "="*80)
            print(f"    {title.upper()}")
            print("="*80)

        def print_stat(label, value, total=None, indent=0):
            prefix = " " * indent
            label_formatted = f"{prefix}{label:<45}"
            value_str = f"{value}"
            if total is not None and total > 0:
                percent = (value / total) * 100
                print(f"{label_formatted} {value_str:<10} ({percent:.2f}%)")
            else:
                print(f"{label_formatted} {value_str}")

        print_header("Rapport d'Analyse Exhaustif du Corpus Poétique")

        # --- Section 1: Vue d'Ensemble ---
        print_header("Vue d'Ensemble du Corpus")
        print_stat("Nombre total de poèmes uniques", self.total_poems)
        print_stat("Nombre total d'auteurs uniques", len(self.authors_data))
        print_stat("Nombre total de recueils IDENTIFIÉS uniques", len(self.collections_by_id))
        print_stat("Nombre de titres de recueils NON-IDENTIFIÉS", len(self.collections_by_title_only))

        # --- Section 2: Qualité des Métadonnées ---
        print_header("Qualité et Complétude des Métadonnées")
        print_stat("Poèmes avec un auteur identifié", self.poems_with_author, self.total_poems)
        print_stat("Poèmes avec une date de publication", self.poems_with_date, self.total_poems)
        print_stat("Poèmes avec un éditeur", self.poems_with_publisher, self.total_poems)
        print_stat("Poèmes avec un traducteur", self.poems_with_translator, self.total_poems)

        # --- Section 3: Analyse Structurelle des Recueils ---
        print_header("Analyse Structurelle des Recueils")
        print_stat("Poèmes liés à un recueil IDENTIFIÉ (par ID)", self.poems_with_identified_collection, self.total_poems)
        print_stat("Poèmes liés à un recueil NON-IDENTIFIÉ (titre)", self.poems_with_unidentified_collection, self.total_poems)
        total_in_collection = self.poems_with_identified_collection + self.poems_with_unidentified_collection
        print_stat("Total des poèmes dans un recueil (tous types)", total_in_collection, self.total_poems)
        print_stat("Poèmes avec une position ordonnée", self.poems_with_order, total_in_collection)
        print_stat("Poèmes avec un titre de section", self.poems_with_section, self.poems_with_identified_collection)
        
        collections_with_sections = sum(1 for data in self.collections_by_id.values() if data["sections"])
        if self.collections_by_id:
            print_stat("Recueils IDENTIFIÉS structurés en sections", collections_with_sections, len(self.collections_by_id))
        
        # --- Section 4: Analyse Quantitative du Contenu ---
        print_header("Analyse Quantitative du Contenu Poétique")
        print_stat("Nombre total de strophes", self.total_stanzas)
        print_stat("Nombre total de vers", self.total_verses)
        if self.total_poems > 0:
            print_stat("Nb. moyen de strophes par poème", f"{self.total_stanzas / self.total_poems:.2f}")
            print_stat("Nb. moyen de vers par poème", f"{self.total_verses / self.total_poems:.2f}")
        
        poem_lengths_verses = [p['verses'] for p in self.poem_lengths_data]
        if poem_lengths_verses:
            print_stat("Longueur médiane des poèmes (en vers)", f"{statistics.median(poem_lengths_verses):.0f}")
            print_stat("Poème le plus long (en vers)", max(poem_lengths_verses))
            print_stat("Poème le plus court (en vers)", min(poem_lengths_verses))

        # --- Section 5: Analyse des Versions et Doublons ---
        print_header("Analyse des Versions et Doublons")
        real_hubs = {k: v for k, v in self.hubs_data.items() if v["version_count"] > 1}
        print_stat("Hubs multi-versions réels (>1 poème)", len(real_hubs))
        exact_duplicates = sum(count - 1 for count in self.checksum_counts.values() if count > 1)
        print_stat("Contenus wikitext strictement identiques (doublons)", exact_duplicates)
        
        # --- Section 6: Classements (Top 10) ---
        print_header("Classements (Top 10)")

        # Auteurs
        print("\n  Auteurs les plus prolifiques (par nb. de poèmes) :")
        top_authors = sorted(self.authors_data.items(), key=lambda item: item[1]['poem_count'], reverse=True)[:10]
        for author, data in top_authors:
            print(f"    - {author:<40} {data['poem_count']} poèmes")

        # Recueils par taille
        print("\n  Recueils IDENTIFIÉS les plus vastes (par nb. de poèmes) :")
        top_collections = sorted(self.collections_by_id.items(), key=lambda item: item[1]['poem_count'], reverse=True)[:10]
        for cid, data in top_collections:
            title = next(iter(data['titles']), f"ID: {cid}")
            print(f"    - {title:<40} {data['poem_count']} poèmes")
        
        print("\n  Titres de recueils NON-IDENTIFIÉS les plus fréquents :")
        for title, count in self.collections_by_title_only.most_common(10):
            print(f"    - {title:<40} {count} poèmes")

        # Recueils par structure
        print("\n  Recueils les mieux structurés (par nb. de sections) :")
        top_structured = sorted(self.collections_by_id.items(), key=lambda item: len(item[1]['sections']), reverse=True)[:10]
        for cid, data in top_structured:
            title = next(iter(data['titles']), f"ID: {cid}")
            print(f"    - {title:<40} {len(data['sections'])} sections")

        # Hubs
        print("\n  Hubs avec le plus de versions :")
        top_hubs = sorted(real_hubs.items(), key=lambda item: item[1]['version_count'], reverse=True)[:10]
        if top_hubs:
            for hub_id, data in top_hubs:
                hub_title_display = data['title'] if data['title'] and 'autonome' not in data['title'] else f"Hub ID {hub_id}"
                print(f"    - {hub_title_display:<40} {data['version_count']} versions")
        else:
            print("    Aucun hub multi-versions trouvé.")
            
        # Poèmes par longueur
        print("\n  Poèmes les plus longs (par nb. de vers) :")
        top_longest = sorted(self.poem_lengths_data, key=lambda p: p['verses'], reverse=True)[:10]
        for poem in top_longest:
            display = f"\"{poem['title']}\" ({poem['author']})"
            print(f"    - {display:<60} {poem['verses']} vers")
        
        print("\n  Poèmes les plus courts (par nb. de vers) :")
        top_shortest = sorted(self.poem_lengths_data, key=lambda p: p['verses'])[:10]
        for poem in top_shortest:
            display_title = f"\"{poem['title']}\" ({poem['author']})"
            print(f"    - {display_title:<60} {poem['verses']} vers")
        
        print("\n" + "="*80)


def main():
    """Point d'entrée : cherche un fichier `cleaned` et lance l'analyse."""
    if len(sys.argv) > 1:
        target = Path(sys.argv[1])
        if not target.exists():
             print(f"[ERREUR] Le fichier spécifié '{target}' est introuvable.", file=sys.stderr)
             sys.exit(1)
    else:
        repo_root = Path(__file__).resolve().parents[2]
        candidates = [
            repo_root / "data" / "poems.cleaned.jsonl.gz",
            repo_root / "data" / "poems.jsonl.gz",
        ]
        target = next((p for p in candidates if p.exists()), None)
    
    if target is None:
        print("[ERREUR] Aucun fichier de données trouvé. Veuillez spécifier le chemin ou placer", file=sys.stderr)
        print("         `poems.cleaned.jsonl.gz` ou `poems.jsonl.gz` dans le répertoire `data/`.", file=sys.stderr)
        sys.exit(1)

    analyzer = CorpusAnalyzer(target)
    analyzer.analyze_and_report()

if __name__ == "__main__":
    main()