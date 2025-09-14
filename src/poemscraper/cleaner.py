"""
Script de Nettoyage et de Déduplication du Corpus.

Ce script traite un fichier de résultats `poems.jsonl.gz` pour :
1. Nettoyer les titres des poèmes.
2. Supprimer les poèmes en double en se basant sur leur `page_id`.
3. Supprimer le champ de métadonnées 'license_name' qui est inutilisé.

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
        
    seen_page_ids: set[int] = set()
    total_read = 0
    duplicates_removed = 0
    written_count = 0

    print(f"[*] Traitement de {input_path}...")

    with open_maybe_gzip(output_path, "wt") as fout:
        for poem in iter_jsonl(input_path):
            total_read += 1
            page_id = poem.get("page_id")
            
            if page_id is None:
                print(f"[AVERTISSEMENT] Poème sans page_id trouvé à la ligne {total_read}, il sera conservé.", file=sys.stderr)
            elif page_id in seen_page_ids:
                duplicates_removed += 1
                continue
            else:
                seen_page_ids.add(page_id)

            processed = process_poem(poem)
            
            fout.write(json.dumps(processed, ensure_ascii=False) + "\n")
            written_count += 1

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