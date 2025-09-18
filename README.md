# **Wikisource Poem Scraper v4**

Un scraper Python 3.10+ robuste, asynchrone et professionnel pour extraire l'intégralité des poèmes structurés, leurs métadonnées, et **la structure complète des recueils** depuis les API Wikisource, dans le respect total des conditions d'utilisation de Wikimedia.

Ce projet utilise `asyncio` et `aiohttp` pour une récupération concurrente contrôlée, `Pydantic` pour la validation de schéma, `BeautifulSoup` pour une analyse HTML fiable, et `SQLite` pour l'indexation.

**Avertissement sur la licence des données :** Le code source de cet outil est sous licence MIT. Cependant, les **données extraites** de Wikisource sont soumises aux licences spécifiées par Wikisource (généralement Domaine Public, CC-BY-SA, etc.). **Il est de votre responsabilité de respecter ces licences dans toute utilisation ultérieure des données.**

## **Fonctionnalités Clés**

* **Scraping Hiérarchique** : Le scraper ne se contente plus de lister les poèmes ; il comprend et modélise la structure des pages de recueils, en conservant **l'ordre des poèmes** et en identifiant les **titres de section**.  
* **Asynchrone et Contrôlé** : Utilise `asyncio` et un sémaphore pour des performances élevées tout en respectant les limites de l'API de Wikisource.  
* **Parsing Robuste** : Analyse le HTML rendu avec `BeautifulSoup` pour extraire la structure du poème, une méthode plus fiable que l'analyse du wikitext seul.  
* **Métadonnées Riches** : Extrait les auteurs, dates, recueils et plus, en combinant les informations structurées (HTML) et les templates wikitext.  
* **Validation de Schéma** : `Pydantic` garantit que toutes les données de sortie sont propres, structurées et conformes au schéma défini.  
* **Sortie Structurée** : Génère un fichier `poems.jsonl.gz` contenant les poèmes enrichis d'informations contextuelles sur leur place dans un recueil, et un index `SQLite` pour des requêtes rapides.  
* **Reprise Intelligente** : Peut reprendre une session de scraping interrompue en ignorant les pages déjà traitées et indexées dans la base de données.  
* **Logging Avancé** : Produit des logs détaillés, y compris des journaux en arborescence pour visualiser le chemin d'exploration de chaque auteur, facilitant le débogage et l'analyse de la qualité.

## **Installation**

1. Clonez ce dépôt et naviguez dans le répertoire :  
    ```bash
   git clone \[https://github.com/sharle4/wikisource-poem-scraper.git\](https://github.com/sharle4/wikisource-poem-scraper.git)  
   cd wikisource-poem-scraper
    ```

2. Créez et activez un environnement virtuel (recommandé) :
    ```bash
   python3 \-m venv .venv  
   source .venv/bin/activate
    ```

3. Installez le projet et ses dépendances :
    ```bash
   pip install \-e ".\[dev\]"
    ```

## **Utilisation**

Le scraper s'utilise via une interface en ligne de commande.

```bash
\# Exemple pour scraper tous les poèmes
python \-m poemscraper \--lang fr \--category "Poèmes par Auteur"
```

### **Arguments Principaux**

* `\--lang` **(requis)** : Code langue du Wikisource à cibler (ex: `fr`, `en`).  
* `\--category` **(requis)** : Catégorie racine de laquelle démarrer (ex: `Poèmes`, `Poètes français...`).  
* `\--output_dir` : Répertoire de sortie (défaut: `./data/`).  
* `\--workers` : Nombre de requêtes API parallèles (défaut: 20).  
* `\--limit` : Limite le nombre total de pages à traiter (utile pour les tests).  
* `\--resume` : Active le mode reprise.  
* `\--tree-log` : Génère des logs d'exploration en arborescence dans `data/logs/tree-logs/`.

## **Structure des Données de Sortie**

Le fichier `poems.jsonl.gz` contient un objet JSON par ligne pour chaque poème. Les nouveaux champs clés liés à la structure des recueils sont :

* `collection_page_id`: L'ID de la page du recueil parent.  
* `collection_title`: Le titre du recueil.  
* `section_title`: Le titre de la section dans laquelle le poème apparaît.  
* `poem_order`: La position (index 0\) du poème dans la séquence du recueil.  
* `collection_structure`: Un objet JSON complet décrivant la structure du recueil. *Note : ce champ n'est inclus que pour le premier poème d'un recueil pour éviter la duplication massive de données.*