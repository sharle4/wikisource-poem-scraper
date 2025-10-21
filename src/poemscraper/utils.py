from __future__ import annotations

import gzip
import io
import json
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
        gz_file = gzip.open(path, mode.replace("t", "") + "b")
        return io.TextIOWrapper(gz_file, encoding="utf-8")
    
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
                print(f"[AVERTISSEMENT] Ligne {line_num} ignorée: impossible de décoder le JSON.", file=sys.stderr)
                continue