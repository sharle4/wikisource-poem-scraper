"""
Nettoie les titres des poèmes dans un JSONL (gzip ou non) en:
- gardant uniquement le dernier segment après les "/"
- supprimant les parenthèses et leur contenu

Exemple: "Le Parnasse contemporain/1869/La Nuit (Aicard)" -> "La Nuit"

Utilisation basique:
  python clean_titles.py --input data/poems.jsonl.gz --output data/poems.cleaned.jsonl.gz

Options:
  --dry-run           N'écrit rien, affiche seulement les stats.
  --input, -i        Chemin du fichier d'entrée (.jsonl ou .jsonl.gz)
  --output, -o       Chemin du fichier de sortie (.jsonl ou .jsonl.gz)
                      Par défaut: <input_basename>.cleaned[.gz]
"""
from __future__ import annotations

import argparse
import gzip
import io
import json
import re
import sys
from pathlib import Path
from typing import Iterator


def is_gz(path: Path) -> bool:
    return path.suffix == ".gz" or path.name.endswith(".jsonl.gz")


def open_maybe_gzip(path: Path, mode: str):
    if "b" in mode:
        return gzip.open(path, mode) if is_gz(path) else open(path, mode)
    if is_gz(path):
        gz = gzip.open(path, mode.replace("t", "b"))
        return io.TextIOWrapper(gz, encoding="utf-8")
    return open(path, mode, encoding="utf-8")


def clean_title(title: str) -> str:
    """Retourne le dernier segment après '/' et enlève tout contenu entre parenthèses.
    Si le résultat est vide, retourne le titre d'origine nettoyé des espaces.
    """
    if not isinstance(title, str):
        return title
    segment = title.split("/")[-1]
    segment = re.sub(r"\s*\([^)]*\)", "", segment)
    segment = re.sub(r"\s+", " ", segment).strip()
    return segment if segment else title.strip()


def iter_jsonl(path: Path) -> Iterator[dict]:
    with open_maybe_gzip(path, "rt") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                try:
                    f.seek(0)
                except Exception:
                    pass
                obj = json.load(open_maybe_gzip(path, "rt"))
                if isinstance(obj, list):
                    for item in obj:
                        yield item
                else:
                    yield obj
                break


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Nettoie les titres des poèmes d'un JSONL (gz supporté)")
    parser.add_argument("--input", "-i", type=Path, required=True, help="Fichier d'entrée (.jsonl ou .jsonl.gz)")
    parser.add_argument("--output", "-o", type=Path, help="Fichier de sortie (.jsonl ou .jsonl.gz)")
    parser.add_argument("--dry-run", action="store_true", help="N'écrit rien, affiche seulement les stats")
    args = parser.parse_args(argv)

    input_path: Path = args.input
    if not input_path.exists():
        print(f"[ERROR] Fichier d'entrée introuvable: {input_path}", file=sys.stderr)
        return 2

    output_path: Path
    if args.output:
        output_path = args.output
    else:
        base = input_path.name
        if base.endswith(".gz"):
            base = base[:-3]
            output_path = input_path.with_name(base + ".cleaned.jsonl.gz")
        else:
            output_path = input_path.with_name(base + ".cleaned.jsonl")

    total = 0
    changed = 0

    if args.dry_run:
        for obj in iter_jsonl(input_path):
            total += 1
            old = obj.get("title")
            new = clean_title(old) if old is not None else old
            if old != new:
                changed += 1
        print(f"[DRY-RUN] Total: {total} | Changés: {changed}")
        return 0

    with open_maybe_gzip(output_path, "wt") as fout:
        for obj in iter_jsonl(input_path):
            total += 1
            if isinstance(obj, dict) and "title" in obj:
                old = obj["title"]
                obj["title"] = clean_title(old)
                if obj["title"] != old:
                    changed += 1
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")

    print(f"[OK] Écrit {total} enregistrements dans: {output_path}")
    print(f"      Titres modifiés: {changed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
