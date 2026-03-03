# Wikisource Poem Scraper v5.0.0

**An advanced, dual-mode Python pipeline for extracting, classifying, and structuring the complete corpus of poetry from Wikisource.**

Version 5.0.0 marks a fundamental architectural milestone: the introduction of a fully offline dump-based processing mode alongside the existing online API-based scraper. Both modes share the same classification brain, the same parsing engine, the same Pydantic-validated output schema, and produce byte-compatible JSONL output files. The online mode is network-bound, asynchronous, and designed for incremental or targeted scraping runs. The offline mode is CPU/IO-bound, sequential-with-multiprocessing, and designed for processing entire Wikimedia database dumps at scale without a single network request.

This tool was built for NLP researchers, computational literary scholars, and data engineers who need a production-grade, structured poetry corpus with rich relational metadata (author attribution, collection membership, section hierarchy, poem ordering, multi-version hub grouping) that goes far beyond flat text extraction.

> **Data License Notice:** The source code of this tool is released under the MIT License. However, the **data extracted** from Wikisource is subject to the licenses specified by Wikisource itself (typically Public Domain, CC-BY-SA 3.0, or GFDL). It is your responsibility to comply with these licenses in any downstream use of the extracted data.

---

## Table of Contents

1. [Project Overview and Key Features](#1-project-overview--key-features)
2. [Setting Up a Wikimedia Bot Account](#2-setting-up-a-wikimedia-bot-account)
3. [Installation and Prerequisites](#3-installation--prerequisites)
4. [Exhaustive CLI Usage Guide](#4-exhaustive-cli-usage-guide)
5. [Output Data and Structure (The PoemSchema)](#5-output-data--structure-the-poemschema)
6. [Deep Dive: Architecture and Inner Workings](#6-deep-dive-architecture--inner-workings)
7. [Technical Justifications and Data Choices](#7-technical-justifications--data-choices)

---

## 1. Project Overview & Key Features

Wikisource Poem Scraper is not a simple web scraper. It is a semantic extraction pipeline that understands the hierarchical structure of literary works on Wikisource: how poems are organized into collections (recueils), how collections are divided into titled sections, and how the same poem can exist in multiple editions (multi-version hubs). It models this entire relational tree and outputs structured, validated data that preserves these relationships.

### Core Features

| Feature | Description |
|---|---|
| **Dual-Mode Architecture** | Online mode uses async HTTP against the MediaWiki API. Offline mode processes local Wikimedia dump files (SQL, XML, NDJSON). Both produce identical output. |
| **Intelligent Page Classification** | Every page is classified as one of six `PageType` values (POEM, POETIC_COLLECTION, MULTI_VERSION_HUB, AUTHOR, DISAMBIGUATION, OTHER) using a multi-signal heuristic engine that reads HTML structure, category membership, and Wikidata annotations. |
| **Structural Collection Parsing** | For collection pages, the engine performs semantic analysis of the rendered HTML to extract the ordered list of poems, detect section boundaries (headings, bold text, definition lists), and build a complete `Collection` object with nested `Section` and `PoemInfo` entries. |
| **Multi-Version Hub Resolution** | Pages that serve as portals to multiple editions of the same poem (e.g., "Le Lac" with 1820 and 1860 editions) are detected and their child versions are extracted, with each version linked back to the hub via `hub_page_id` and `hub_title`. |
| **Dual-Source Metadata Extraction** | Metadata is extracted from both the rendered HTML (using `itemprop` microdata attributes: author, datePublished, isPartOf, publisher, translator) and the raw wikitext (using `mwparserfromhell` to parse templates like `{{Titre}}`, `{{InfoÉdit}}`, `{{Auteur}}`). The HTML source takes precedence, with wikitext serving as a fallback. |
| **Author Name Normalization** | A dedicated `author_cleaner` module strips prefixes like "Auteur:", removes noise phrases ("non signé", "auteur du texte"), fixes spacing around punctuation, and applies a curated normalization dictionary to unify variant spellings. |
| **Strict Schema Validation** | Every output record is validated against a Pydantic `PoemSchema` model before writing. Fields that fail validation raise errors that are logged and counted, never silently dropped. |
| **Resume Capability** | Both modes support `--resume`. The SQLite index of already-processed `page_id` values is loaded at startup, and pages already in the index are skipped. |
| **Hierarchical Tree Logging** | When `--tree-log` is enabled, the exploration path is recorded as a tree structure per author category, output as both human-readable `.txt` files and machine-readable `.json` files for visualization and debugging. |
| **Post-Processing Pipeline** | Built-in `clean`, `enrich`, `analyze`, and `debug` subcommands allow you to deduplicate, enrich collection IDs, compute corpus statistics, and extract diagnostic subsets without writing any code. |

---

## 2. Setting Up a Wikimedia Bot Account

### Why You Need a Bot Account

The Wikimedia Foundation imposes strict rate limits on anonymous and standard-account API requests. An unauthenticated client is typically limited to a handful of concurrent requests and risks receiving HTTP 429 (Too Many Requests) or outright IP bans when performing sustained scraping at scale. An authenticated bot account provides several advantages:

- **Higher rate limits.** Bot-flagged accounts can issue requests at a higher sustained rate. The `maxlag` parameter becomes more permissive.
- **Increased `cmlimit`/`cllimit` values.** Standard accounts can request at most 500 results per API call. Bot accounts can request up to 5,000 results per call (using `cmlimit=max`), drastically reducing the number of round-trips needed to enumerate large category trees.
- **`assert=bot` privileges.** Some API actions can use `assert=bot` to confirm the authenticated session, providing clearer error handling.
- **Compliance with Wikimedia policy.** The [Wikimedia API Etiquette](https://www.mediawiki.org/wiki/API:Etiquette) strongly recommends identifying your client with a descriptive User-Agent and authenticating when performing bulk operations. This scraper sets a User-Agent header automatically, but authentication demonstrates good faith.

### Step-by-Step: Creating a Bot Password

1. **Log in to your Wikisource account.** Navigate to the Wikisource project you intend to scrape (e.g., `https://fr.wikisource.org`). If you do not have an account, create one.

2. **Navigate to Special:BotPasswords.** Go to `https://fr.wikisource.org/wiki/Special:BotPasswords` (or the equivalent for your language project). This page allows you to create application-specific passwords that are separate from your main account password.

3. **Create a new bot password.**
   - Enter a **bot name** (e.g., `PoemScraper`). This will be combined with your username to form the login name: `YourUsername@PoemScraper`.
   - Select the **grants** (permissions) the bot needs. For this scraper, the minimum required grants are:
     - **Basic rights** — Needed for authentication.
     - **High-volume editing** — Not strictly required for reading, but recommended if you intend to use the `maxlag` parameter and high-concurrency fetches.
   - Click **Create**.

4. **Record your credentials.** The system will display a bot password. This is shown only once. Record both:
   - **Bot username:** `YourUsername@PoemScraper`
   - **Bot password:** The generated password string.

5. **Configure the scraper.** Create a `.env` file at the root of the project directory:

   ```env
   WIKISOURCE_BOT_USERNAME=YourUsername@PoemScraper
   WIKISOURCE_BOT_PASSWORD=your_generated_bot_password
   ```

   The scraper loads these automatically via `python-dotenv` at startup. Alternatively, you can pass them as CLI arguments `--bot-user` and `--bot-pass`.

### Standard Account vs. Bot-Flagged Account

| Aspect | Standard Account | Bot-Flagged Account |
|---|---|---|
| API `cmlimit` / `cllimit` max | 500 | 5,000 |
| Concurrent request tolerance | Low (~1-3 RPS sustained) | Higher (~10+ RPS sustained) |
| Risk of 429/IP ban | High during bulk scraping | Lower (recognized as legitimate automation) |
| `maxlag` behavior | Less permissive | More permissive |
| Requires bureaucrat approval | No | Depends on wiki policy |

> **Note:** Having a Bot Password (via `Special:BotPasswords`) does *not* automatically give you a "bot-flagged" account. Bot-flagging is a separate user right granted by bureaucrats and primarily affects edit-related features, not read-only API access. However, using a Bot Password for authentication still provides measurable improvements in rate-limit tolerance for read operations, because the MediaWiki server recognizes you as an identified, consenting automated client rather than an anonymous IP.

---

## 3. Installation & Prerequisites

### Python Version

This project requires **Python 3.10 or later** and is tested up to Python 3.12. The `pyproject.toml` specifies `requires-python = ">=3.10, <3.13"`.

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/sharle4/wikisource-poem-scraper.git
   cd wikisource-poem-scraper
   ```

2. Create and activate a virtual environment:

   ```bash
   python -m venv .venv
   source .venv/bin/activate    # Linux/macOS
   .venv\Scripts\activate       # Windows
   ```

3. Install the project with all dependencies:

   ```bash
   pip install -e .
   ```

   For development (includes pytest, black, mypy, flake8):

   ```bash
   pip install -e ".[dev]"
   ```

### Dependencies

All dependencies are declared in `pyproject.toml`:

| Package | Version | Purpose |
|---|---|---|
| `aiohttp[speedups]` | >= 3.9.0 | Async HTTP client for the MediaWiki API and live HTML fetching (online mode) |
| `aiosqlite` | >= 0.19.0 | Async SQLite interface for the online mode's database operations |
| `backoff` | >= 2.2.0 | Exponential backoff and retry for transient API failures |
| `pydantic` | >= 2.7.0 | Data validation and serialization of the `PoemSchema` output |
| `tqdm` | >= 4.66.0 | Progress bars for all long-running operations |
| `mwparserfromhell` | >= 0.6.0 | MediaWiki wikitext parser for template-based metadata extraction |
| `beautifulsoup4` | >= 4.12.0 | HTML parsing for poem structure extraction and page classification |
| `lxml` | >= 5.0.0 | Fast HTML parser backend for BeautifulSoup |
| `python-dotenv` | >= 1.0.0 | Loads `.env` files for bot credentials |

No additional dependencies are required for offline mode. The XML parsing uses Python's built-in `xml.etree.ElementTree`, the SQL dump parsing uses a custom state-machine parser, and multiprocessing uses `concurrent.futures.ProcessPoolExecutor`.

### Offline Mode Prerequisites

To run the offline mode, you must first download the following Wikimedia dump files and place them in a single directory (the `--dumps-dir` path):

#### Required Dump Files

**NDJSON Enterprise HTML Dumps (namespace 0):**

These are Wikimedia Enterprise HTML dumps containing the fully rendered HTML for all namespace-0 pages. They are available from the Wikimedia Enterprise dumps at `https://dumps.wikimedia.org/other/enterprise_html/`.

| File Pattern | Count | Approx. Size |
|---|---|---|
| `frwikisource_namespace_0_0.ndjson` through `frwikisource_namespace_0_9.ndjson` | 10 files | ~21 GB total |

**XML Article Dumps:**

Standard MediaWiki XML exports containing raw wikitext for all pages across all namespaces. Download from `https://dumps.wikimedia.org/frwikisource/latest/`.

| File Pattern | Count | Approx. Size |
|---|---|---|
| `frwikisource-latest-pages-articles1.xml-p1p1500000` | 4 split files | ~11.4 GB total |
| `frwikisource-latest-pages-articles1.xml-p1500001p3000000` | | |
| `frwikisource-latest-pages-articles1.xml-p3000001p4500000` | | |
| `frwikisource-latest-pages-articles1.xml-p4500001p4790859` | | |

**SQL Database Dumps:**

Relational metadata dumps from `https://dumps.wikimedia.org/frwikisource/latest/`.

| File | Approx. Size | Purpose |
|---|---|---|
| `frwikisource-latest-page.sql` | ~726 MB | Page metadata: page_id, title, namespace, redirect flag |
| `frwikisource-latest-categorylinks.sql` | ~743 MB | Category membership links (references `linktarget`) |
| `frwikisource-latest-linktarget.sql` | ~887 MB | Link target table (modern MediaWiki schema for category name resolution) |
| `frwikisource-latest-redirect.sql` | ~4.5 MB | Redirect mappings |

> **Total disk space required for dumps:** approximately 35 GB. The output index database (`dump_index.sqlite`) will be an additional 2-4 GB during the indexing phase.

---

## 4. Exhaustive CLI Usage Guide

The scraper provides a unified CLI with five subcommands. After installation, it is accessible via:

```bash
python -m poemscraper <command> [options]
```

or, if installed as a script:

```bash
poemscraper <command> [options]
```

### Global Options

These options apply to all subcommands:

| Option | Type | Default | Description |
|---|---|---|---|
| `--verbose` / `-v` | flag | `false` | Enable DEBUG-level logging to stdout. Without this flag, the log level is INFO. |
| `--bot-user` | string | `$WIKISOURCE_BOT_USERNAME` | Bot username for API authentication. Falls back to the environment variable if not provided. |
| `--bot-pass` | string | `$WIKISOURCE_BOT_PASSWORD` | Bot password for API authentication. Falls back to the environment variable if not provided. |

### 4.1 `scrape` — The Main Extraction Command

This is the primary command. It discovers pages, classifies them, extracts poem structures, resolves collection hierarchies, and writes the final output.

```bash
poemscraper scrape --lang <code> --category <name> [options]
```

#### Arguments

| Option | Type | Required | Default | Description |
|---|---|---|---|---|
| `--lang` | string | Yes | — | Language code of the Wikisource project to target (e.g., `fr` for French, `en` for English). |
| `--category` | string | Yes | — | Root category from which to start the extraction. In online mode, this is typically an author-grouping category like `"Poèmes par Auteur"`. In offline mode, this is used for category-tree traversal in the SQL index (e.g., `"Poèmes"`). |
| `--output_dir` | path | No | `./data/` | Directory where all output files will be written. Created automatically if it does not exist. |
| `--workers` | int | No | `3` | **Online mode:** Number of concurrent async HTTP requests (controls the `asyncio.Semaphore`). **Offline mode:** Number of CPU cores used for the `ProcessPoolExecutor` during HTML parsing and page classification. |
| `--limit` | int | No | `None` | Maximum number of pages to process. Useful for testing and development. When set, only the first N pages (by discovery order in online mode, or by page_id sort in offline mode) are processed. |
| `--resume` | flag | No | `false` | Resume a previously interrupted scraping session. Loads all `page_id` values from the existing `poems_index.sqlite` and skips pages that were already processed. Output files are opened in append mode. |
| `--tree-log` | flag | No | `false` | Generate hierarchical exploration tree logs in `<output_dir>/logs/tree-logs/`. Each author category gets a `.txt` (human-readable tree) and `.json` (machine-readable) log file. |
| `--mode` | choice | No | `online` | Scraping mode. `online` uses the MediaWiki API and live website HTML. `offline` processes local dump files. |
| `--dumps-dir` | path | No | `None` | Path to the directory containing Wikimedia dump files. **Required when `--mode offline` is used.** |

#### Example: Online Mode

```bash
# Full scrape of French Wikisource poems with bot auth and 15 workers
poemscraper scrape \
  --lang fr \
  --category "Poèmes par Auteur" \
  --output_dir ./data \
  --workers 15 \
  --tree-log

# Quick test run: only process the first 50 pages
poemscraper scrape \
  --lang fr \
  --category "Poèmes par Auteur" \
  --limit 50

# Resume an interrupted session
poemscraper scrape \
  --lang fr \
  --category "Poèmes par Auteur" \
  --workers 15 \
  --resume
```

#### Example: Offline Mode

```bash
# Full offline extraction from local dumps using 8 CPU cores
poemscraper scrape \
  --lang fr \
  --category "Poèmes" \
  --mode offline \
  --dumps-dir ./dumps \
  --output_dir ./data \
  --workers 8

# Offline mode with limit for testing
poemscraper scrape \
  --lang fr \
  --category "Poèmes" \
  --mode offline \
  --dumps-dir ./dumps \
  --limit 1000 \
  --workers 4

# Resume an interrupted offline session
poemscraper scrape \
  --lang fr \
  --category "Poèmes" \
  --mode offline \
  --dumps-dir ./dumps \
  --resume
```

> **Note:** In offline mode, the `--bot-user` and `--bot-pass` arguments are silently ignored, as no network requests are made.

#### The Resume Mechanism

The `--resume` flag works identically in both modes:

1. At startup, the scraper checks if `poems_index.sqlite` exists in the output directory.
2. If it exists, all `page_id` values are loaded into a set in memory.
3. During processing, any page whose `page_id` is already in this set is skipped.
4. Output files (`poems.jsonl.gz`, `poems.cleaned.jsonl.gz`) are opened in append mode (`"at"`), so new records are added without overwriting existing ones.
5. In offline mode, the `dump_index.sqlite` (the SQL dump index) is also preserved and reused if it already exists.

This mechanism allows you to interrupt a long-running scrape with `Ctrl+C` and restart it later without losing progress.

### 4.2 `enrich` — Collection Page ID Enrichment

This post-processing command fills in missing `collection_page_id` values by querying the MediaWiki API for poems that have a `collection_title` but no corresponding ID.

```bash
poemscraper enrich \
  --input data/poems.cleaned.jsonl.gz \
  --output data/poems.enriched.jsonl.gz \
  --lang fr \
  --workers 3
```

| Option | Type | Required | Default | Description |
|---|---|---|---|---|
| `--input` / `-i` | path | Yes | — | Input JSONL file to enrich. |
| `--output` / `-o` | path | Yes | — | Output file for the enriched data. |
| `--lang` | string | Yes | — | Language code (needed to construct the API endpoint). |
| `--workers` | int | No | `3` | Number of concurrent API requests for page title resolution. |

The enrichment process works in three phases:
1. **Cache building:** Reads the input file once and builds a mapping of `collection_title` to `collection_page_id` from poems that already have both values.
2. **API resolution:** For titles that have no corresponding ID in the cache, queries the MediaWiki API to find the page ID, resolving redirects.
3. **Writing:** Re-reads the input file and writes the enriched output, filling in previously missing `collection_page_id` values from the combined cache.

> **Note:** In offline mode, the enrichment is performed inline during Phase 3 of the pipeline using the local SQLite index, making this command unnecessary. The `enrich` subcommand is designed for online-mode output.

### 4.3 `clean` — Deduplication and Cleaning

Deduplicates the output by `page_id`, retaining the version with the richest collection context, and applies title normalization.

```bash
poemscraper clean \
  --input data/poems.jsonl.gz \
  --output data/poems.cleaned.jsonl.gz
```

| Option | Type | Required | Description |
|---|---|---|---|
| `--input` / `-i` | path | Yes | Input JSONL file. |
| `--output` / `-o` | path | Yes | Output file for cleaned data. |

The cleaning process:

1. **Title normalization:** Extracts the last path segment after `/` (e.g., `"Les Fleurs du mal/Spleen"` becomes `"Spleen"`), then removes parenthetical annotations (e.g., `"Le Lac (1820)"` becomes `"Le Lac"`).
2. **Deduplication:** When multiple records share the same `page_id`, the version that has a non-null `collection_page_id` is preferred over one without it. This handles the race condition in online mode where a poem might be processed once independently and once with collection context.
3. **Metadata cleanup:** Removes deprecated metadata fields (e.g., `license_name`).

### 4.4 `analyze` — Corpus Statistics

Generates a comprehensive statistical report of the extracted corpus.

```bash
# Analyze a specific file
poemscraper analyze data/poems.cleaned.jsonl.gz

# Auto-detect the most recent file in data/
poemscraper analyze
```

| Option | Type | Required | Description |
|---|---|---|---|
| `filepath` | path | No | Path to the JSONL file to analyze. If omitted, the tool searches for the most recently modified `.jsonl.gz` file in the `data/` directory. |

The report includes:

- **Corpus overview:** Total unique poems, unique authors, identified and unidentified collections.
- **Metadata completeness:** Percentage of poems with author, publication date, publisher, and translator.
- **Structural analysis:** Poems linked to identified collections (by page_id), poems linked by title only, poems with ordinal positions, poems with section titles.
- **Content analysis:** Total stanzas, total verses, averages, median, longest and shortest poems.
- **Version and duplicate analysis:** Multi-version hub counts, exact wikitext duplicates (by SHA-256 checksum), collection titles shared across multiple page IDs.
- **Top-10 rankings:** Most prolific authors, largest collections, most structured collections (by section count), hubs with the most versions, longest and shortest poems.

### 4.5 `debug` — Extract Unidentified Collection Poems

Extracts poems that belong to a collection (have a `collection_title`) but lack a `collection_page_id`, for manual analysis and debugging.

```bash
poemscraper debug \
  --input data/poems.enriched.jsonl.gz \
  --output data/debug.unidentified.jsonl.gz
```

| Option | Type | Required | Description |
|---|---|---|---|
| `--input` / `-i` | path | Yes | Input JSONL file to analyze. |
| `--output` / `-o` | path | Yes | Output file for the extracted poems. |

---

## 5. Output Data & Structure (The PoemSchema)

### Output Files

Every scraping run produces three output files in the specified `--output_dir`:

| File | Format | Description |
|---|---|---|
| `poems.jsonl.gz` | Gzipped JSONL | The raw, complete output. Contains one JSON object per line for every extracted poem, including potential duplicates from the online mode's re-queuing mechanism. |
| `poems.cleaned.jsonl.gz` | Gzipped JSONL | The deduplicated, title-normalized version. Produced automatically in both modes (equivalent to running `clean` manually). One unique poem per `page_id`. |
| `poems_index.sqlite` | SQLite database | A lightweight index containing one row per poem with key fields for fast lookups: `page_id`, `title`, `author`, `publication_date`, `language`, `checksum_sha256`, `extraction_timestamp`, `collection_page_id`, `collection_title`, `section_title`, `poem_order`, `hub_title`, `hub_page_id`. |

### The PoemSchema

Every record in the output files conforms to the following Pydantic model:

```json
{
  "page_id": 1234567,
  "revision_id": 9876543,
  "title": "Les Fleurs du mal/Spleen",
  "language": "fr",
  "wikisource_url": "https://fr.wikisource.org/wiki/Les_Fleurs_du_mal/Spleen",

  "collection_page_id": 1234000,
  "collection_title": "Les Fleurs du mal",
  "section_title": "Spleen et Idéal",
  "poem_order": 14,
  "hub_title": null,
  "hub_page_id": 1234567,

  "metadata": {
    "author": "Charles Baudelaire",
    "publication_date": "1857",
    "source_collection": "Les Fleurs du mal",
    "publisher": "Poulet-Malassis et de Broise",
    "translator": null
  },

  "structure": {
    "stanzas": [
      [
        "Quand le ciel bas et lourd pèse comme un couvercle",
        "Sur l'esprit gémissant en proie aux longs ennuis,",
        "Et que de l'horizon embrassant tout le cercle",
        "Il nous verse un jour noir plus triste que les nuits ;"
      ],
      [
        "Quand la terre est changée en un cachot humide,",
        "Où l'Espérance, comme une chauve-souris,",
        "S'en va battant les murs de son aile timide",
        "Et se cognant la tête à des plafonds pourris ;"
      ]
    ],
    "raw_markers": [
      "<div class=\"poem\">"
    ]
  },

  "collection_structure": null,

  "normalized_text": "Quand le ciel bas et lourd pèse comme un couvercle\nSur l'esprit gémissant en proie aux longs ennuis,\nEt que de l'horizon embrassant tout le cercle\nIl nous verse un jour noir plus triste que les nuits ;\n\nQuand la terre est changée en un cachot humide,\nOù l'Espérance, comme une chauve-souris,\nS'en va battant les murs de son aile timide\nEt se cognant la tête à des plafonds pourris ;",

  "raw_wikitext": "<poem>\nQuand le ciel bas et lourd...\n</poem>",

  "checksum_sha256": "a1b2c3d4e5f6...",
  "extraction_timestamp": "2025-01-15T14:30:00.000000+00:00",
  "provenance": "api"
}
```

### Field Reference

#### Identity Fields

| Field | Type | Description |
|---|---|---|
| `page_id` | `int` | The MediaWiki page ID. This is the primary identifier and deduplication key across the entire corpus. |
| `revision_id` | `int` | The specific MediaWiki revision ID that was extracted. Pinpoints the exact version of the page at extraction time. |
| `title` | `string` | The canonical page title as stored in MediaWiki (e.g., `"Les Fleurs du mal/Spleen"`). In the cleaned output, this is normalized to the last path segment (e.g., `"Spleen"`). |
| `language` | `string` | The Wikisource language code (e.g., `"fr"`). |
| `wikisource_url` | `string (URL)` | The canonical URL to the poem's page on Wikisource. |

#### Collection Hierarchy Fields

These fields model the relational structure of the poem within a collection:

| Field | Type | Description |
|---|---|---|
| `collection_page_id` | `int` or `null` | The `page_id` of the parent collection (recueil) page. `null` if the poem was not found to belong to any collection, or if the collection page could not be resolved. |
| `collection_title` | `string` or `null` | The title of the parent collection. This may be populated even when `collection_page_id` is null, if the poem's metadata references a collection by name but the scraper could not find the corresponding page. |
| `section_title` | `string` or `null` | The title of the section within the collection where this poem appears (e.g., `"Spleen et Idéal"` within *Les Fleurs du mal*). Only populated for poems whose parent collection has structured sections. |
| `poem_order` | `int` or `null` | The zero-based ordinal position of this poem within its collection's ordered sequence. Allows reconstruction of the original reading order. |
| `hub_title` | `string` or `null` | If this poem is a specific edition of a multi-version work, this is the title of the hub page (e.g., `"Le Lac"` for the editions `"Le Lac (1820)"` and `"Le Lac (1860)"`). |
| `hub_page_id` | `int` | Unique group identifier. If the poem belongs to a multi-version hub, this is the hub's `page_id`. If the poem is standalone, this equals the poem's own `page_id`. Useful for grouping all editions of the same work. |
| `collection_structure` | `Collection` or `null` | A complete JSON object describing the full structure of the parent collection (sections, poem titles, page IDs, URLs). **This field is only populated for the first poem in each collection** to avoid massive data duplication. All other poems in the same collection have this set to `null`. |

#### Metadata Fields

| Field | Type | Description |
|---|---|---|
| `metadata.author` | `string` or `null` | The poem's author, extracted from HTML `itemprop="author"` or wikitext templates. Normalized by `author_cleaner`. |
| `metadata.publication_date` | `string` or `null` | Year or date of publication, typically extracted from `itemprop="datePublished"` or the `{{InfoÉdit}}` template's `ANNÉE` parameter. |
| `metadata.source_collection` | `string` or `null` | The name of the source collection or publication, extracted from `itemprop="isPartOf"` or the `RECUEIL` template parameter. If neither source provides it and the page title contains a `/`, the parent path segment is used as a heuristic (e.g., `"Les Contemplations"` from `"Les Contemplations/Demain, dès l'aube"`). |
| `metadata.publisher` | `string` or `null` | The publishing house, if found in HTML microdata. |
| `metadata.translator` | `string` or `null` | The translator, if the work is a translation. |

#### Content Fields

| Field | Type | Description |
|---|---|---|
| `structure.stanzas` | `List[List[str]]` | The poem's text as a nested array: a list of stanzas, where each stanza is a list of verse strings. This is the parsed, structured representation extracted from the rendered HTML. |
| `structure.raw_markers` | `List[str]` | The opening HTML tags of the poem blocks that were detected (e.g., `"<div class=\"poem\">"` or `"<poem>"`). Useful for debugging the parser. |
| `normalized_text` | `string` | The complete poem text as a single flat string. Verses are separated by `\n`, stanzas by `\n\n`. This is the primary field for NLP applications. |
| `raw_wikitext` | `string` | The complete raw wikitext source of the page revision. Contains the original MediaWiki markup with templates, transclusion directives, and formatting tags. Useful for linguistic analysis of editorial conventions. |

#### Technical Fields

| Field | Type | Description |
|---|---|---|
| `checksum_sha256` | `string` | SHA-256 hash of the `raw_wikitext` field. Used for deduplication and integrity checking across multiple extraction runs. |
| `extraction_timestamp` | `string (ISO 8601)` | UTC timestamp of when the poem was extracted. |
| `provenance` | `"api"` or `"dump"` | Indicates whether the data was extracted via the online API mode (`"api"`) or the offline dump mode (`"dump"`). This is the only field that differs between modes. |

---

## 6. Deep Dive: Architecture & Inner Workings

### A. The Shared Brain (Classification & Parsing)

The core intellectual engine of this scraper is shared between both modes. It is composed of three modules that are never modified between online and offline paths: `classifier.py`, `parsing.py`, and `processors.py`.

#### The PageType Taxonomy

Every page in the Wikisource namespace-0 is classified into one of the following types:

| PageType | Meaning | What Happens |
|---|---|---|
| `POEM` | A leaf-level page containing actual poem text. | Processed by `PoemProcessor` to extract structure, metadata, and output a `PoemSchema` record. |
| `POETIC_COLLECTION` | A table-of-contents page listing poems in a collection (recueil). | Not extracted as a poem itself. Instead, its ordered list of poem links and section titles is parsed and used to enrich the poems it references with collection context. |
| `MULTI_VERSION_HUB` | A portal page linking to multiple editions of the same work. | Not extracted as a poem. Its sub-page links are extracted and each version is processed independently, linked back via `hub_page_id`. |
| `AUTHOR` | An author page (namespace 102 on frwikisource). | Skipped. |
| `DISAMBIGUATION` | A disambiguation page. | Skipped. |
| `OTHER` | Anything else that does not match the above signals. | Skipped. |
| `SECTION_TITLE` | Not a page type per se, but used internally when parsing collection structure. | Used to mark section boundaries within a collection's ordered link list. |

#### The Classification Algorithm

The `PageClassifier` operates on a multi-signal heuristic system. It receives four inputs: the page metadata dict, the parsed HTML (BeautifulSoup), the language code, and the parsed wikicode (mwparserfromhell). Classification proceeds as follows:

1. **Namespace check.** If the page is not in namespace 0, it is immediately classified as AUTHOR (if it starts with the localized author prefix) or OTHER.

2. **Category signals.** The page's categories are extracted from the page metadata. Two category memberships are checked:
   - If the page belongs to `"Recueils de poèmes"` (Poetry Collections) → `POETIC_COLLECTION`.
   - If the page belongs to `"Éditions multiples"` (Multiple Editions) → `MULTI_VERSION_HUB`.

3. **HTML structural signals.** Six signals are extracted from the parsed HTML:
   - `has_donnees_structurees`: Presence of a Wikidata link (`<a title="d:Q...">`) indicating structured data annotation.
   - `has_editions_header`: Presence of an `<h2>` or `<h3>` heading containing "Éditions".
   - `has_ws_summary`: Presence of a `<div class="ws-summary">` element (Wikisource summary box).
   - `has_toc`: Presence of a `<div id="toc">` table of contents.
   - `has_poem_structure`: Whether `PoemParser.extract_poem_structure()` finds any `<div class="poem">` or `<poem>` blocks.

4. **Decision tree.** The signals are evaluated in priority order:
   - Category signals take absolute precedence.
   - Structural indicators (`ws-summary`, `toc`, `editions_header`) combined with `has_donnees_structurees` indicate a `MULTI_VERSION_HUB`; without Wikidata, they indicate a `POETIC_COLLECTION`.
   - If poem structure is found, the page is a `POEM`.
   - Wikidata links combined with list items (`<ul><li>`) suggest a `MULTI_VERSION_HUB`.
   - If no signals match, the page is `OTHER`.

#### The Poem Parser

`PoemParser.extract_poem_structure()` scans the BeautifulSoup tree for elements with `class="poem"` (either `<div>` or `<span>`), or `<poem>` tags. For each poem block:

1. The text content is extracted using `get_text(separator="\n", strip=True)`.
2. Stanzas are split on double newlines (`\n\n`).
3. Within each stanza, individual verses are split on single newlines.
4. Empty lines and whitespace-only lines are stripped.

The result is a `PoemStructure` object containing the nested `stanzas` array and the `raw_markers` list (the opening tag of each detected poem block).

#### The Collection Structure Parser

`PageClassifier.extract_ordered_collection_links()` is a sophisticated semantic parser for collection table-of-contents pages. It:

1. Locates the main content area (`.mw-parser-output`).
2. Iterates through all block-level elements: headings, paragraphs, lists, and divs.
3. For each element, determines whether it is:
   - A **section title** (headings `<h1>`-`<h6>`, definition list terms `<dt>`, or elements containing bold/strong/italic text without valid poem links).
   - A **poem link** (an `<a>` tag with a valid `/wiki/` href, a title attribute, and no internal-namespace prefix).
4. Returns an ordered list of `(title, PageType)` tuples preserving the document order.

This parser correctly handles diverse collection formats found on Wikisource: flat lists of poems, nested sections with headings, definition lists, and mixed structures with inline section markers.

#### The Poem Processor

`PoemProcessor.process()` takes the classified page data and produces the final `PoemSchema`:

1. Extracts the poem structure using `PoemParser`.
2. Extracts HTML-based metadata via `itemprop` attributes (author, date, collection, publisher, translator).
3. Extracts wikitext-based metadata by parsing templates with `mwparserfromhell` (Titre, InfoÉdit, Auteur).
4. Merges metadata sources with HTML taking priority over wikitext.
5. Applies author name normalization.
6. Computes the normalized text from the structured stanzas.
7. Assigns collection context (page_id, title, section, order) and hub context if provided.
8. Computes the SHA-256 checksum of the raw wikitext.
9. Constructs and returns the validated `PoemSchema` object.

### B. The Online Pipeline (Network Bound)

The online mode (`--mode online`, the default) uses an asynchronous Producer-Consumer architecture built on `asyncio`.

#### Architecture Overview

```
                    ┌────────────────────┐
                    │     Producer       │
                    │  (Category Tree    │
                    │   Crawler)         │
                    └────────┬───────────┘
                             │ page_ids
                             ▼
                    ┌────────────────────┐
                    │   asyncio.Queue    │
                    └────────┬───────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
     ┌────────────┐ ┌────────────┐ ┌────────────┐
     │ Consumer 1 │ │ Consumer 2 │ │ Consumer N │
     │ (Classify  │ │ (Classify  │ │ (Classify  │
     │  & Process)│ │  & Process)│ │  & Process)│
     └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
           │              │              │
           └──────────────┼──────────────┘
                          │ PoemSchema objects
                          ▼
                 ┌─────────────────┐
                 │ queue.Queue     │
                 │ (thread-safe)   │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │ Writer Thread   │
                 │ (Sync I/O:      │
                 │  JSONL + SQLite) │
                 └─────────────────┘
```

#### The Producer

The producer (`_producer` method) crawls the category tree:

1. Normalizes the root category title using the localized prefix (e.g., `Catégorie:Poèmes par Auteur`).
2. Enumerates all subcategories using `get_subcategories_generator()` with pagination.
3. Filters to non-empty categories using batch `get_category_info()` calls (50 titles per API request).
4. For each non-empty author category, enumerates all pages using `get_pages_in_category_generator()`.
5. Each discovered page is wrapped in a queue item with parent context and pushed to the `asyncio.Queue`.

The producer respects the `--limit` argument and stops discovery once enough pages are queued.

#### The Consumers

N consumer tasks (controlled by `--workers`) run concurrently, each processing pages from the shared queue:

1. **Fetch page data.** `get_resolved_page_data(page_id)` retrieves the wikitext, categories, URL, and revision ID in a single API call, automatically resolving redirects.
2. **Fetch rendered HTML.** `get_rendered_html(page_title)` fetches the full rendered HTML from the live Wikisource website (not the API's `action=parse`, but the actual wiki page URL). This ensures the HTML includes all ProofreadPage transclusion resolutions.
3. **Parse and classify.** The HTML is parsed with BeautifulSoup, the wikitext with mwparserfromhell, and the page is classified.
4. **Route by type:**
   - **POEM:** Processed by `PoemProcessor.process()` and the result is put on the writer queue.
   - **POETIC_COLLECTION:** The collection's ordered links are extracted. Each referenced poem title is resolved to a page_id via a batch API call, then queued for processing with full collection context (collection object, section title, poem order).
   - **MULTI_VERSION_HUB:** Sub-page titles are extracted and each version is queued with hub context.

#### Intelligent Re-Queuing

The online mode uses a sophisticated re-queuing mechanism (`_schedule_page_if_new`) to handle the case where a poem is discovered twice: once from its author category (without collection context) and once from a collection page (with context). The mechanism tracks which page_ids already have collection context and allows a re-queue only if the new queue item provides context that the original did not. This ensures the final output includes the richest possible metadata without processing cycles.

#### Rate Limiting and Retry

The `WikiAPIClient` implements multiple layers of protection:

- **Semaphore.** A `asyncio.Semaphore` limits the number of concurrent in-flight requests (set by `--workers`).
- **Token-bucket throttle.** A sliding window tracks the last 10 request timestamps. If 10 requests were issued in less than 1 second, the client sleeps until the 1-second window expires. This enforces a hard ceiling of 10 requests per second.
- **Exponential backoff.** The `backoff` library wraps every API call with exponential retry for transient errors (HTTP 500, 502, 503, 504, connection errors, timeouts). Permanent errors (404, 403) are not retried.
- **429 handling.** If the server returns HTTP 429, the client reads the `Retry-After` header and sleeps for the specified duration before retrying.
- **Expensive endpoint detection.** If any API request takes longer than 1 second, the client waits an additional 5 seconds before the next request to avoid overloading the server with expensive queries.

#### The Writer Thread

A dedicated synchronous writer thread consumes `PoemSchema` objects from a `queue.Queue` and performs all disk I/O:

1. Serializes the poem to JSON and appends it to `poems.jsonl.gz`.
2. If cleaned output is enabled, applies `process_poem()` (title normalization, deduplication by page_id) and writes to `poems.cleaned.jsonl.gz`.
3. Inserts the index row into `poems_index.sqlite` using `INSERT OR REPLACE`.

This separation ensures that async I/O tasks are never blocked by disk writes, and that all SQLite operations happen on a single thread (SQLite's preferred concurrency model).

### C. The Offline Pipeline (CPU/IO Bound)

The offline mode (`--mode offline`) is a fundamentally different architecture: a sequential, multi-phase batch pipeline that processes local Wikimedia dump files without any network I/O. It is implemented in `offline_core.py` as the `OfflineOrchestrator` class.

#### Phase Overview

```
Phase 1          Phase 2           Phase 3          Phase 4          Phase 5
┌──────────┐    ┌──────────────┐  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ SQL Dumps │    │ NDJSON Files │  │ In-Memory    │ │ XML Dumps    │ │ Write Output │
│ → SQLite  │───▶│ → Classify   │─▶│ Enrichment   │▶│ → Wikitext   │▶│ → JSONL+DB   │
│ Index     │    │ (multiproc)  │  │              │ │              │ │              │
└──────────┘    └──────────────┘  └──────────────┘ └──────────────┘ └──────────────┘
  ~10 min          ~30 min          seconds           ~15 min          ~5 min
```

#### Phase 1: Dump Indexing

**Input:** Four SQL dump files (~3.4 GB total).
**Output:** A SQLite database (`dump_index.sqlite`) with four tables and indexes.

The `DumpIndexBuilder` class parses each SQL dump file and inserts the data into a local SQLite database optimized for fast lookups:

1. **SQL Parsing.** The dump files contain MySQL `INSERT INTO ... VALUES (...),(...),...;` statements. A custom character-by-character state machine parser (`_parse_sql_values` in `dump_readers.py`) tokenizes each value tuple, correctly handling:
   - Single-quoted strings with MySQL escape sequences (`\'`, `\\`, `\n`, `\r`, `\t`, `\0`).
   - `NULL` values.
   - Integer and floating-point numbers.
   - **Binary string decoding.** MediaWiki's `varbinary` columns store UTF-8 bytes. The SQL dump file itself is encoded in Latin-1 (ISO-8859-1), so French characters like `è`, `é`, `ç` appear as multi-byte sequences. The parser reads the file as Latin-1, extracts the raw string between quotes, then re-encodes it as Latin-1 bytes and decodes as UTF-8 to recover the correct Unicode text.

2. **Table creation.** Four tables are created:
   - `pages` (page_id, title, namespace, is_redirect, page_len)
   - `linktarget` (lt_id, lt_namespace, lt_title)
   - `categorylinks` (cl_from, cl_target_id, cl_type)
   - `redirects` (rd_from, rd_namespace, rd_title)

3. **Bulk loading.** Inserts are batched in groups of 50,000 rows within explicit transactions. SQLite PRAGMAs are configured for maximum write speed: `journal_mode=WAL`, `synchronous=OFF`, `cache_size=-512000` (512 MB).

4. **Deferred indexing.** Indexes are created after all bulk inserts are complete. This is significantly faster than inserting with indexes active, because SQLite does not need to update B-tree structures on every insert.

5. **Category tree traversal.** Using the indexed database, a BFS (breadth-first search) traverses the category tree starting from the root category (e.g., `"Poèmes"`):
   - Find the root category's `lt_id` in the `linktarget` table (namespace 14).
   - Find all subcategory `page_id` values via `categorylinks` where `cl_target_id` matches and `cl_type = 'subcat'`.
   - For each subcategory page, look up its title in `pages`, find the corresponding `lt_id` in `linktarget`, and recurse.
   - Track visited `lt_id` values to handle circular references.
   - Collect all leaf-page `page_id` values (`cl_type = 'page'`).
   - Filter to namespace 0 only.

> **Why `linktarget`?** Modern MediaWiki (post-2022) changed the `categorylinks` table schema. The old `cl_to` column (which stored the category name as text) was replaced by `cl_target_id`, a foreign key to the new `linktarget` table. This means you cannot simply `SELECT cl_to FROM categorylinks` to find category names. You must join `categorylinks.cl_target_id` with `linktarget.lt_id` and read `linktarget.lt_title` to resolve the category name. This is a critical implementation detail that naive parsers miss.

#### Phase 2: NDJSON Streaming and Classification

**Input:** 10 NDJSON files (~21 GB total), plus the SQLite index from Phase 1.
**Output:** In-memory dictionaries of pending poems, collections, and hubs.

This phase streams through the Enterprise HTML dump files, which contain the fully rendered HTML for every namespace-0 page. The HTML in these dumps includes resolved ProofreadPage transclusions, meaning `<pages index="..." from=X to=Y />` directives have been expanded into actual poem text with `<div class="poem">` blocks.

1. **Category pre-fetch.** Before streaming, all categories for every target page_id are batch-queried from the SQLite index. This avoids per-page database queries during the hot loop.

2. **Regex pre-filter.** For each line in the NDJSON file, the `"identifier"` field (which appears early in the JSON, before the massive `article_body.html` field) is extracted using a compiled regex on the raw bytes: `r'"identifier"\s*:\s*(\d+)'`. If the extracted integer is not in the target page_id set, the line is skipped without performing a full `json.loads()`. This optimization reduces processing time by an order of magnitude, since only a small fraction of the ~500,000 namespace-0 pages are in the target set.

3. **Parallel classification.** Pages that pass the pre-filter are deserialized and submitted in batches to a `ProcessPoolExecutor` with `--workers` processes. Each worker:
   - Parses the HTML with `BeautifulSoup(html, "lxml")`.
   - Creates a minimal empty wikicode object (wikitext is not available from NDJSON).
   - Builds a `page_data` dict compatible with the shared `PageClassifier`.
   - Runs classification and returns a serializable result dict.

4. **Result routing.** Back on the main thread, each classification result is routed:
   - **POEM:** The page_data and raw HTML are stored in `poems_pending` (keyed by page_id). The HTML string is stored instead of the BeautifulSoup object to save memory (it will be re-parsed in Phase 5).
   - **POETIC_COLLECTION:** The ordered links are stored in a `collections` dict. Each poem title in the collection is resolved to a page_id using the `title_to_id` index. If a referenced poem was not in the initial target set, its page_id is added to `discovered_page_ids`.
   - **MULTI_VERSION_HUB:** Sub-page titles are resolved and stored similarly.

5. **Second pass.** If any pages were discovered through collection/hub references that were not in the initial category-based target set, a second NDJSON pass is performed with only those page_ids. This bounded two-pass approach mirrors the online mode's dynamic re-queuing without requiring unbounded iteration.

#### Phase 3: Context Enrichment

**Input:** The in-memory `poems_pending`, `collections`, and `hubs` dicts.
**Output:** A `poem_collection_context` mapping: `poem_page_id` to `{collection_page_id, collection_title, section_title, poem_order, is_first_poem_in_collection, collection_obj, hub_title, hub_page_id}`.

This phase replaces the online mode's queue-based context passing with a deterministic reverse-index:

1. For each collection, iterate through its ordered links and build `Collection`, `Section`, and `PoemInfo` objects (the same Pydantic models used by the online mode).
2. For each poem found in a collection, record its collection page_id, section title, and ordinal position.
3. The first poem in each collection gets the complete `Collection` object for the `collection_structure` field.
4. For each hub, map each sub-page page_id to the hub's title and page_id.
5. For poems whose title contains a `/` and whose parent path matches a known collection title, infer the collection membership (a heuristic fallback, same as the online mode's `source_collection` inference).

#### Phase 4: XML Wikitext Extraction

**Input:** 4 XML dump files (~11.4 GB total), plus the set of page_ids from `poems_pending`.
**Output:** A `wikitext_dict` mapping: `page_id` to `wikitext_string`.

This phase streams through the MediaWiki XML export dumps using `xml.etree.ElementTree.iterparse`:

1. Each XML file is processed using `iterparse` with `events=("end",)`, listening for `<page>` elements in the `{http://www.mediawiki.org/xml/export-0.11/}` namespace.
2. For each `<page>` element, the `<ns>`, `<id>`, and `<revision><text>` sub-elements are extracted.
3. If the page_id is not in the target set, the element is cleared immediately and skipped.
4. After extracting the wikitext, `elem.clear()` is called on the page element and periodically on the root element to prevent memory accumulation. This maintains constant memory usage regardless of file size.
5. The files are processed in filename-suffix order to respect page_id ranges.

> **Why not extract wikitext from the NDJSON?** The NDJSON Enterprise HTML dumps contain only the rendered HTML (`article_body.html`). There is no `wikitext` field. The raw wikitext is needed for two critical purposes: (1) computing the `checksum_sha256` of the source text, which is the deduplication and integrity key; (2) populating the `raw_wikitext` output field, which NLP researchers use for analysis of editorial markup conventions and template usage. Only the XML dumps contain this data.

#### Phase 5: Finalization and Writing

**Input:** `poems_pending`, `poem_collection_context`, `wikitext_dict`, `collections`.
**Output:** `poems.jsonl.gz`, `poems.cleaned.jsonl.gz`, `poems_index.sqlite`.

Poems are processed in deterministic order (sorted by page_id) to ensure reproducibility:

1. For each pending poem, the wikitext from Phase 4 is injected into the `page_data["revisions"][0]["content"]` field.
2. The raw HTML is re-parsed with BeautifulSoup (stored as a string in Phase 2 to save memory).
3. If wikitext was found, it is parsed with `mwparserfromhell`. Otherwise, an empty wikicode is used.
4. Collection and hub context from Phase 3 is assembled into the appropriate arguments.
5. `PoemProcessor.process()` is called with the full data, producing a `PoemSchema` object.
6. The `provenance` field is overridden to `"dump"`.
7. The poem is serialized and written to the gzipped JSONL output, the cleaned output (with deduplication), and the SQLite index.

---

## 7. Technical Justifications & Data Choices

### Why Parse HTML Instead of Just Wikitext?

The naive approach to extracting poem text from Wikisource would be to parse the raw wikitext from the XML dumps and look for `<poem>` tags or `<pages index="..." />` transclusion directives. This approach fails catastrophically on Wikisource because of the **ProofreadPage extension**.

On Wikisource, the majority of poem text is not written inline in the main article's wikitext. Instead, the main article contains a transclusion directive like:

```wikitext
<pages index="Baudelaire - Les Fleurs du mal.djvu" from=42 to=43 />
```

This directive tells MediaWiki to pull the text from specific proofread scan pages (in namespace 104 "Page:") and insert it into the rendered output. The raw wikitext of the main article contains only the transclusion directive, not the actual poem text. The poem text itself lives on separate Page: namespace pages that correspond to individual scanned pages of the original book.

When MediaWiki renders the page (either on the live site or in the Enterprise HTML dumps), it resolves these transclusions and produces the full HTML with `<div class="poem">` blocks containing the actual verses. This is why the scraper must parse the **rendered HTML** to get the poem text, not the raw wikitext.

The raw wikitext is still valuable for metadata extraction (templates like `{{Titre}}` and `{{InfoÉdit}}` are present in the main article's wikitext) and for the `checksum_sha256` integrity field, which is why both data sources are used.

### Why Require Both NDJSON and XML in Offline Mode?

The offline mode requires two separate dump formats because each contains data the other lacks:

| Data | NDJSON Enterprise HTML | XML Article Dump |
|---|---|---|
| Fully rendered HTML (with transclusion resolution) | **Yes** — `article_body.html` | No |
| Raw wikitext source | **No** — not included in this dump format | **Yes** — `<revision><text>` |
| Page metadata (title, page_id, revision_id) | Yes | Yes |
| Namespace coverage | Namespace 0 only | All namespaces |

The NDJSON provides the resolved transclusions (the actual poem text as rendered HTML), which is the primary data source for poem structure extraction. The XML provides the raw wikitext, needed for template-based metadata extraction, the `raw_wikitext` output field, and the SHA-256 checksum.

Using both formats in sequence (NDJSON in Phase 2 for HTML, XML in Phase 4 for wikitext) produces output that is fully equivalent to what the online mode obtains from two separate API calls per page (`get_resolved_page_data` for wikitext and `get_rendered_html` for HTML).

### Why the `linktarget` Table in SQL?

In 2022, the MediaWiki development team introduced a normalization change to the database schema. Previously, the `categorylinks` table stored the category name directly in a `cl_to TEXT` column. This was replaced by a numeric foreign key `cl_target_id` that references the new `linktarget` table.

The old schema:
```sql
-- Old: direct category name storage
SELECT cl_from FROM categorylinks WHERE cl_to = 'Poèmes';
```

The new schema:
```sql
-- New: requires a join through linktarget
SELECT cl.cl_from
FROM categorylinks cl
JOIN linktarget lt ON cl.cl_target_id = lt.lt_id
WHERE lt.lt_namespace = 14 AND lt.lt_title = 'Poèmes';
```

This change was made for storage efficiency (storing a 4-byte integer instead of a variable-length string per row) and to normalize the schema. However, it means that any tool parsing modern MediaWiki SQL dumps must download and parse the `linktarget.sql` dump file in addition to `categorylinks.sql`. The `DumpIndexBuilder` handles this by loading the linktarget table first, creating an index on `(lt_namespace, lt_title)`, and then using it for all subsequent category resolution queries.

### Why a Local SQLite Index Instead of In-Memory Dictionaries?

The SQL dumps contain data for hundreds of thousands of pages, millions of category links, and millions of link targets. Loading all of this into Python dictionaries would consume many gigabytes of RAM. By using SQLite as an intermediate indexed store, the pipeline:

- Keeps memory usage predictable and bounded (SQLite manages its own page cache).
- Enables complex queries (multi-table joins for category resolution) that would be cumbersome with nested dictionaries.
- Persists the index to disk, allowing resume without re-parsing the SQL dumps.
- Leverages SQLite's B-tree indexes for sub-millisecond lookups on page_id and category title.

### Why ProcessPoolExecutor for HTML Parsing?

BeautifulSoup's `lxml`-based HTML parsing is CPU-intensive work that is bound by Python's Global Interpreter Lock (GIL). Using `ThreadPoolExecutor` would not provide true parallelism for this workload. `ProcessPoolExecutor` spawns separate Python processes, each with its own GIL, allowing genuine parallel HTML parsing across multiple CPU cores. The trade-off is the serialization overhead for passing data between processes, which is why the worker function returns serializable dicts rather than BeautifulSoup or Pydantic objects.

### Why Store Raw HTML Strings Instead of BeautifulSoup Objects?

During Phase 2, pending poems could number in the tens of thousands. Storing a `BeautifulSoup` object for each (which holds the full parsed DOM tree with parent/child references, attribute dicts, and NavigableString objects) would consume several gigabytes of memory. Instead, the raw HTML string is stored (typically 50-200 KB per page) and re-parsed in Phase 5 when the poem is actually processed. This doubles the parsing work for poems but keeps memory usage under control for the expected corpus size.

---

## License

The source code of this project is released under the **MIT License**. See the `LICENSE` file for details.

The data extracted from Wikisource is subject to the licenses specified by Wikisource (typically Public Domain for works whose authors died more than 70 years ago, CC-BY-SA 3.0 for editorial contributions). It is your responsibility to comply with these licenses.
