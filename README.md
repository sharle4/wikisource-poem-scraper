# Wikisource Poem Scraper

Un scraper Python 3.10+ robuste, asynchrone et professionnel pour extraire l'intégralité des poèmes structurés et de leurs métadonnées depuis les API Wikisource, dans le respect total des conditions d'utilisation de Wikimedia.

Ce projet utilise `asyncio`, `aiohttp` et un sémaphore pour une récupération concurrente contrôlée, `Pydantic` pour la validation de schéma, et `SQLite` pour l'indexation.

**Avertissement sur la licence des données :** Ce script est un outil d'extraction. Le code source de cet outil est sous licence MIT. Cependant, les **données extraites** de Wikisource sont soumises aux licences spécifiées par Wikisource (généralement une combinaison de Domaine Public, CC-BY-SA 3.0, et d'autres licences libres). **Il est de votre responsabilité de respecter ces licences dans toute utilisation ultérieure des données.**

## Fonctionnalités

* **Asynchrone et contrôlé :** Utilise `asyncio` et `aiohttp` avec un sémaphore pour limiter la charge sur l'API (évite les 429).
* **Parsing de structure :** Analyse le Wikitext (`<poem>`) pour extraire la structure canonique (strophes/vers) plutôt que de se fier au HTML rendu fragile.
* **Métadonnées riches :** Tente d'extraire les auteurs, licences et dates depuis les templates et catégories (via `mwparserfromhell`).
* **Validation de schéma :** Utilise Pydantic pour garantir que 100% des données en sortie sont conformes au schéma défini.
* **Sortie multiple :** Génère un fichier `poems.jsonl.gz` (NDJSON compressé) pour le stockage de données brutes et un index `poems_index.sqlite` pour des recherches rapides.
* **Reprise (Checkpointing) :** Utilise la base SQLite pour suivre les `page_id` déjà traités, permettant de reprendre une extraction interrompue.

## Installation

1.  Clonez ce dépôt :
    ```bash
    git clone [https://github.com/sharle4/wikisource-poem-scraper.git](https://github.com/sharle4/wikisource-poem-scraper.git)
    cd wikisource-poem-scraper
    ```

2.  Il est fortement recommandé d'utiliser un environnement virtuel :
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  Installez le projet et ses dépendances (définis dans `pyproject.toml`) :
    ```bash
    # Installe le paquet en mode "éditable" avec les dépendances de dev
    pip install -e ".[dev]"
    ```

4.  (Optionnel) Configurez les outils de qualité de code (pré-configurés dans `pyproject.toml` et `.flake8`):
    ```bash
    # Lancer les tests
    pytest
    
    # Formater le code
    black src/ tests/
    isort src/ tests/
    
    # Vérifier le linting et le typage
    flake8 src/ tests/
    mypy src/
    ```

## Utilisation (CLI)

Le module peut être exécuté directement.

```bash
python -m poemscraper [OPTIONS]

Arguments :

--lang (requis) : Code langue du Wikisource à cibler (ex: fr, en, de).

--category (requis) : Catégorie racine de laquelle démarrer le crawl (ex: Poèmes, Poésie).

--output_dir : Répertoire de sortie pour les fichiers (Défaut: ./data/).

--workers : Nombre de workers concurrents (requêtes API parallèles). (Défaut: 5).

--limit : Limiter le nombre total de poèmes à extraire (utile pour les tests).

--resume : Active le mode reprise. Ignore les page_id déjà présents dans l'index SQLite.

--dry-run : Analyse les catégories et identifie les pages, mais n'effectue pas l'extraction complète.