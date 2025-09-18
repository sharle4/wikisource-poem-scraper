"""
Script de Nettoyage et de Déduplication du Corpus.

Ce script traite un fichier de résultats `poems.jsonl.gz` pour :
1. Nettoyer les titres des poèmes.
2. Supprimer les poèmes en double en se basant sur leur `page_id`, en conservant
   intelligemment la version la plus complète (celle avec des informations de recueil).
3. Supprimer les champs de métadonnées inutilisés.

Utilisation:
  python -m src.poemscraper.cleaner --input <fichier_entree.jsonl.gz> --output <fichier_sortie.jsonl.gz>
"""
from __future__ import annotations

import argparse
import gzip
import io
import json
import re
import sys
from pathlib import Path
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
    """Itère sur les lignes d'un fichier JSONL."""
    with open_maybe_gzip(path, "rt") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                print(f"[AVERTISSEMENT] Ligne {line_num} ignorée: impossible de décoder le JSON.", file=sys.stderr)
                continue

def clean_title(title: str) -> str:
    """Retourne le dernier segment après '/' et enlève tout contenu entre parenthèses."""
    if not isinstance(title, str):
        return title
    segment = title.split("/")[-1]
    segment = re.sub(r"\s*\([^)]*\)", "", segment)
    segment = re.sub(r"\s+", " ", segment).strip()
    return segment if segment else title.strip()

def process_poem(poem: Dict[str, Any]) -> Dict[str, Any]:
    """Applique toutes les transformations à un objet poème."""
    if "title" in poem:
        poem["title"] = clean_title(poem["title"])
    
    if "metadata" in poem and isinstance(poem["metadata"], dict):
        poem["metadata"].pop("license_name", None)

    return poem

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Nettoie et déduplique un fichier de résultats poems.jsonl.gz.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--input", "-i", type=Path, required=True, help="Fichier d'entrée (.jsonl ou .jsonl.gz)")
    parser.add_argument("--output", "-o", type=Path, required=True, help="Fichier de sortie (.jsonl ou .jsonl.gz)")
    args = parser.parse_args(argv)

    input_path: Path = args.input
    output_path: Path = args.output

    if not input_path.exists():
        print(f"[ERREUR] Fichier d'entrée introuvable: {input_path}", file=sys.stderr)
        return 1

    if output_path.exists():
        print(f"[AVERTISSEMENT] Le fichier de sortie {output_path} existe déjà et sera écrasé.", file=sys.stderr)
        
    best_poems: Dict[int, Dict[str, Any]] = {}
    total_read = 0
    
    print(f"[*] Traitement de {input_path}...")
    print("[*] Phase 1: Lecture et sélection de la meilleure version pour chaque poème...")

    for poem in iter_jsonl(input_path):
        total_read += 1
        page_id = poem.get("page_id")
        
        if page_id is None:
            print(f"[AVERTISSEMENT] Poème sans page_id trouvé à la ligne {total_read}, ignoré.", file=sys.stderr)
            continue

        cleaned_poem = process_poem(poem)
        
        existing_poem = best_poems.get(page_id)
        
        if not existing_poem or (cleaned_poem.get("collection_page_id") is not None and existing_poem.get("collection_page_id") is None):
            best_poems[page_id] = cleaned_poem

    print(f"[*] Phase 2: Écriture des {len(best_poems)} poèmes uniques et optimaux dans {output_path}...")
    
    written_count = 0
    with open_maybe_gzip(output_path, "wt") as fout:
        for poem in best_poems.values():
            fout.write(json.dumps(poem, ensure_ascii=False) + "\n")
            written_count += 1

    duplicates_removed = total_read - written_count

    print("\n" + "="*50)
    print(" " * 15 + "RAPPORT DE NETTOYAGE")
    print("="*50)
    print(f"{'Total des poèmes lus:':<35} {total_read}")
    print(f"{'Doublons (basés sur page_id) supprimés:':<35} {duplicates_removed}")
    print("-" * 50)
    print(f"{'Total des poèmes uniques écrits:':<35} {written_count}")
    print(f"[OK] Fichier nettoyé sauvegardé dans : {output_path}")
    print("="*50)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())