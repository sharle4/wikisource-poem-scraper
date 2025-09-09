import logging
import re
from pathlib import Path
from threading import Lock
from typing import Any, Dict

from .classifier import PageType

logger = logging.getLogger(__name__)


def _sanitize_filename(name: str) -> str:
    """Nettoie une chaîne pour en faire un nom de fichier valide."""
    name = re.sub(r'[<>:"/\\|?*]', "_", name)
    name = name.replace(" ", "_").lower()
    return f"{name}.txt"


class HierarchicalLogger:
    """
    Construit et écrit des journaux d'exploration en arborescence pour chaque auteur.
    Cette classe est conçue pour être thread-safe.
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
        self, author_cat: str, parent_title: str, page_title: str, page_type: PageType, reason: str
    ):
        """Ajoute une page (nœud) à l'arborescence de son auteur, avec la raison de sa classification."""
        with self._lock:
            author_tree = self.trees.setdefault(
                author_cat, {"name": author_cat, "children": {}}
            )

            parent_node = self._find_node(author_tree, parent_title)

            if not parent_node:
                parent_node = author_tree

            if page_title not in parent_node["children"]:
                parent_node["children"][page_title] = {
                    "name": page_title,
                    "type": page_type.name,
                    "reason": reason,
                    "children": {},
                }

    def _write_tree_recursive(
        self, file, node: Dict, prefix: str = "", is_last: bool = True
    ):
        """Écrit récursivement l'arborescence dans un fichier avec les bons préfixes et la raison."""
        connector = "└── " if is_last else "├── "
        reason_str = f" ({node.get('reason', '')})" if node.get('reason') else ""
        file.write(f"{prefix}{connector}{node['name']} [{node['type']}{reason_str}]\n")

        child_prefix = "    " if is_last else "│   "
        children = list(node.get("children", {}).values())
        for i, child in enumerate(children):
            self._write_tree_recursive(
                file, child, prefix + child_prefix, i == len(children) - 1
            )

    def write_log_files(self):
        """Écrit tous les arbres construits dans leurs fichiers respectifs."""
        logger.info(f"Writing {len(self.trees)} exploration tree logs...")
        for author_cat, tree in self.trees.items():
            filename = _sanitize_filename(author_cat.split(":")[-1])
            filepath = self.log_dir / filename
            try:
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(f"{author_cat}\n")
                    children = list(tree.get("children", {}).values())
                    for i, child in enumerate(children):
                        self._write_tree_recursive(
                            f, child, "", i == len(children) - 1
                        )
            except Exception as e:
                logger.error(f"Failed to write log file {filepath}: {e}")