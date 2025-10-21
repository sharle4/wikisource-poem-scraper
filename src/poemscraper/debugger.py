"""
Outil de débogage pour extraire les poèmes de recueils non identifiés.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from tqdm import tqdm

from .utils import iter_jsonl, open_maybe_gzip


def extract_unidentified_collections(input_path: Path, output_path: Path):
    """
    Lit un fichier de données de poèmes et extrait ceux qui ont un collection_title
    mais pas de collection_page_id, pour faciliter l'analyse.
    """
    if not input_path.exists():
        print(f"[ERREUR] Fichier d'entrée introuvable: {input_path}", file=sys.stderr)
        return 1

    print(f"[*] Analyse de '{input_path}' pour extraire les poèmes de recueils non identifiés...")
    
    unidentified_count = 0
    
    total_lines = sum(1 for _ in open_maybe_gzip(input_path, "rt"))
    
    with open_maybe_gzip(output_path, "wt") as fout, tqdm(total=total_lines, desc="Analyse des poèmes", unit=" poème") as pbar:
        for poem in iter_jsonl(input_path):
            has_title = poem.get("collection_title") is not None
            has_id = poem.get("collection_page_id") is not None

            if has_title and not has_id:
                fout.write(json.dumps(poem, ensure_ascii=False) + "\n")
                unidentified_count += 1
            pbar.update(1)
    
    print("\n" + "="*50)
    print(" " * 15 + "RAPPORT D'EXTRACTION DE DEBUG")
    print("="*50)
    print(f"{'Total des poèmes lus:':<40} {total_lines}")
    print(f"{'Poèmes extraits (recueil non identifié):':<40} {unidentified_count}")
    print(f"[OK] Fichier de debug sauvegardé dans : {output_path}")
    print("="*50)
    
    return 0

def main(argv: list[str] | None = None) -> int:
    """Point d'entrée pour le script de debug."""
    parser = argparse.ArgumentParser(
        description="Extrait les poèmes appartenant à des recueils non identifiés pour le débogage.",
    )
    parser.add_argument("--input", "-i", type=Path, required=True, help="Fichier d'entrée (ex: data/poems.enriched.jsonl.gz).")
    parser.add_argument("--output", "-o", type=Path, required=True, help="Fichier de sortie pour les poèmes extraits (ex: data/debug.unidentified.jsonl.gz).")
    args = parser.parse_args(argv)

    return extract_unidentified_collections(args.input, args.output)

if __name__ == "__main__":
    sys.exit(main())