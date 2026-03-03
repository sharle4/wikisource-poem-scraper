"""
Script de Nettoyage et de Déduplication du Corpus.

Ce script traite un fichier de résultats `poems.jsonl.gz` pour :
1. Nettoyer les titres des poèmes.
2. Supprimer les poèmes en double en se basant sur leur `page_id`, en conservant
   intelligemment la version la plus complète (celle avec des informations de recueil).
3. Supprimer les champs de métadonnées inutilisés.

Utilisation:
  python -m poemscraper clean --input <fichier_entree.jsonl.gz> --output <fichier_sortie.jsonl.gz>
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, Any

from .utils import iter_jsonl, open_maybe_gzip


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
    """Point d'entrée principal pour la logique de nettoyage."""
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
            print(f"[AVERTISSEMENT] Poème sans page_id trouvé (ligne ~{total_read}), ignoré.", file=sys.stderr)
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
    try:
        sys.exit(main())
    except Exception as e:
        print(f"Erreur inattendue: {e}", file=sys.stderr)
        sys.exit(1)
