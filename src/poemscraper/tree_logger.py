import logging
import re
import json
from pathlib import Path
from threading import Lock
from typing import Any, Dict
from datetime import datetime

from .classifier import PageType

logger = logging.getLogger(__name__)


def _sanitize_filename(name: str) -> str:
    """Nettoie une chaîne pour en faire un nom de fichier valide."""
    name = re.sub(r'[<>:"/\\|?*]', "_", name)
    name = name.replace(" ", "_").lower()
    return name


class HierarchicalLogger:
    """
    Construit et écrit des journaux d'exploration en arborescence pour chaque auteur,
    à la fois en format texte et en format JSON pour la visualisation.
    """

    def __init__(self, log_dir: Path):
        self.log_dir = log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.trees: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()

    def _find_node(self, tree: Dict, title: str) -> Dict | None:
        """Trouve récursivement un nœud dans l'arbre par son titre."""
        if tree.get("name") == title:
            return tree
        for child in tree.get("children", {}).values():
            found = self._find_node(child, title)
            if found:
                return found
        return None
    
    def add_node(
        self, author_cat: str, parent_title: str, page_title: str, page_type: PageType, reason: str, timestamp: datetime
    ):
        """Ajoute une page (nœud) à l'arborescence de son auteur."""
        with self._lock:
            author_tree = self.trees.setdefault(
                author_cat, {"name": author_cat, "children": {}}
            )

            parent_node = self._find_node(author_tree, parent_title)

            if not parent_node:
                parent_node = author_tree
            
            if "children" not in parent_node:
                parent_node["children"] = {}

            if page_title not in parent_node["children"]:
                parent_node["children"][page_title] = {
                    "name": page_title,
                    "type": page_type.name,
                    "reason": reason,
                    "timestamp": timestamp.isoformat(),
                    "children": [],
                }

    def _count_descendants(self, node: Dict) -> int:
        """Compte récursivement tous les descendants d'un nœud."""
        count = len(node.get("children", []))
        for child in node.get("children", []):
            count += self._count_descendants(child)
        return count

    def _convert_children_dict_to_list(self, node: Dict):
        """Convertit récursivement les dictionnaires d'enfants en listes pour la sortie JSON."""
        if "children" in node and isinstance(node["children"], dict):
            node["children"] = list(node["children"].values())
            for child in node["children"]:
                self._convert_children_dict_to_list(child)

    def _write_tree_recursive_txt(
        self, file, node: Dict, prefix: str = "", is_last: bool = True
    ):
        """Écrit récursivement l'arborescence au format texte."""
        connector = "└── " if is_last else "├── "
        timestamp = node.get('timestamp', '')
        full_type = f"{node.get('type', '')} ({node.get('reason', '')})"
        file.write(f"{prefix}{connector}{timestamp} - {node['name']} [{full_type}]\n")

        child_prefix = "    " if is_last else "│   "
        children = node.get("children", [])
        for i, child in enumerate(children):
            self._write_tree_recursive_txt(
                file, child, prefix + child_prefix, i == len(children) - 1
            )
            
    def write_log_files(self):
        """Écrit tous les arbres construits dans leurs fichiers respectifs (TXT et JSON)."""
        logger.info(f"Writing {len(self.trees)} exploration tree logs...")
        for author_cat, tree in self.trees.items():
            self._convert_children_dict_to_list(tree)
            
            filename_base = _sanitize_filename(author_cat.split(":")[-1])
            
            filepath_txt = self.log_dir / f"{filename_base}.txt"
            try:
                with open(filepath_txt, "w", encoding="utf-8") as f:
                    direct_children = len(tree.get("children", []))
                    total_descendants = self._count_descendants(tree)
                    
                    f.write(f"--- {author_cat} ---\n")
                    f.write(f"Direct sub-pages explored: {direct_children}\n")
                    f.write(f"Total descendants found: {total_descendants}\n\n")

                    children = tree.get("children", [])
                    for i, child in enumerate(children):
                        self._write_tree_recursive_txt(
                            f, child, "", i == len(children) - 1
                        )
            except Exception as e:
                logger.error(f"Failed to write TXT log file {filepath_txt}: {e}")

            filepath_json = self.log_dir / f"{filename_base}.json"
            try:
                with open(filepath_json, "w", encoding="utf-8") as f:
                    tree["direct_children"] = len(tree.get("children", []))
                    tree["total_descendants"] = self._count_descendants(tree)
                    json.dump(tree, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.error(f"Failed to write JSON log file {filepath_json}: {e}")
