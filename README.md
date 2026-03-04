<div align="center">

# 📜 Scriptorium v5

**Build structured corpora from Wikisource — online or offline.**

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Wikisource](https://img.shields.io/badge/source-fr.wikisource.org-orange.svg)](https://fr.wikisource.org)

</div>

---

A Python tool that discovers poems through category-tree traversal on Wikisource, classifies each page by type, extracts verse structure from rendered HTML, and writes validated JSONL output. It handles collections (with section titles and reading order), multi-version hubs, and author normalization automatically.

Two modes cover different needs. **Online mode** queries the MediaWiki API and fetches live HTML — ideal for incremental or targeted runs. **Offline mode** processes local Wikimedia dump files (SQL, XML, NDJSON) with zero network I/O — ideal for building the full corpus in one batch. Both share the same classification engine and produce identical output schemas; only the `provenance` field differs (`"api"` vs `"dump"`).

> **Data license:** Source code is MIT-licensed. Extracted data is subject to Wikisource's own licenses (typically Public Domain or CC-BY-SA).

## ✨ Features

- 🔄 **Dual-mode pipeline** — online (async API) or offline (local dumps), same output schema
- 📚 **Collection-aware** — resolves poems to parent collections with section titles and reading order
- 🔀 **Hub support** — groups multiple editions of the same poem under a single hub
- 👤 **Author normalization** — cleans names from HTML microdata and wikitext templates
- 📊 **Structured output** — stanzas, verses, metadata, and normalized text in Pydantic-validated JSONL
- 🔁 **Resumable** — SQLite index tracks progress; interrupted runs pick up where they left off
- 🤖 **Bot authentication** — higher API rate limits via Wikisource bot account
- 🧹 **Post-processing** — built-in deduplication, enrichment, and corpus analysis commands
- 🏆 **Golden Record merging** — reconcile online and offline outputs into the ultimate corpus, keeping the richest version of every poem automatically

## 🚀 Quickstart

```bash
git clone https://github.com/sharle4/wikisource-poem-scraper.git
cd wikisource-poem-scraper
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e .
```

**Online mode:**

```bash
wikisourcescraper scrape --lang fr --category "Poèmes par Auteur" --workers 15
```

**Offline mode:**

```bash
wikisourcescraper scrape --lang fr --category "Poèmes" --mode offline --dumps-dir ./dumps --workers 8
```

**Resume an interrupted run:**

```bash
wikisourcescraper scrape --lang fr --category "Poèmes" --resume
```

## 🔧 Setup & Credentials

<details>
<summary><strong>Online mode — Bot account (recommended)</strong></summary>

Anonymous API requests are rate-limited. A bot account gives higher limits and signals legitimate automated use to Wikimedia.

1. Log in to your Wikisource account (e.g., `fr.wikisource.org`). Create one if needed.
2. Go to **Special:BotPasswords** (`https://fr.wikisource.org/wiki/Special:BotPasswords`).
3. Enter a bot name (e.g., `Scraper`). Your login will be `YourUsername@Scraper`.
4. Grant **Basic rights** and **High-volume editing**. Click **Create**.
5. Save the generated password (shown once).

Create a `.env` file at the project root:

```env
WIKISOURCE_BOT_USERNAME=YourUsername@Scraper
WIKISOURCE_BOT_PASSWORD=the_generated_password
```

Or pass `--bot-user` and `--bot-pass` on the command line.

</details>

<details>
<summary><strong>Offline mode — Dump files (~35 GB)</strong></summary>

Offline mode needs three types of dump files in a single directory.

**SQL + XML dumps** — freely available from `https://dumps.wikimedia.org/frwikisource/latest/`:

| File | Size | Content |
|---|---|---|
| `frwikisource-latest-page.sql` | ~726 MB | Page metadata |
| `frwikisource-latest-categorylinks.sql` | ~743 MB | Category membership |
| `frwikisource-latest-linktarget.sql` | ~887 MB | Category name resolution |
| `frwikisource-latest-redirect.sql` | ~4.5 MB | Redirects |
| `frwikisource-latest-pages-articles1.xml-*` (4 files) | ~11.4 GB | Raw wikitext |

**HTML NDJSON dumps** — require a free [Wikimedia Enterprise](https://enterprise.wikimedia.com/) account:

```powershell
# Edit credentials in the script first
.\dumps\download_enterprise_dumps.ps1
```

Downloads `frwikisource_namespace_0_*.ndjson` (10 files, ~21 GB) with fully rendered HTML for every namespace-0 page.

**Total disk space:** ~35 GB for dumps + 2–4 GB for the generated index.

</details>

---

<details>
<summary><strong>📦 Output Data Structure</strong></summary>

### Output files

Each run produces three files in `--output_dir`:

| File | Description |
|---|---|
| `poems.jsonl.gz` | One JSON per line per poem (may contain duplicates in online mode) |
| `poems.cleaned.jsonl.gz` | Deduplicated, title-normalized. One unique poem per `page_id`. |
| `poems_index.sqlite` | SQLite index for fast lookups and resume support |

### Example record

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
      ["Quand le ciel bas et lourd pèse comme un couvercle", "..."],
      ["Quand la terre est changée en un cachot humide,", "..."]
    ],
    "raw_markers": ["<div class=\"poem\">"]
  },
  "collection_structure": null,
  "normalized_text": "Quand le ciel bas et lourd...\n\nQuand la terre est changée...",
  "raw_wikitext": "<poem>\nQuand le ciel bas et lourd...\n</poem>",
  "checksum_sha256": "a1b2c3d4...",
  "extraction_timestamp": "2025-01-15T14:30:00+00:00",
  "provenance": "api"
}
```

### Key fields

| Field | Description |
|---|---|
| `normalized_text` | Flat text. Verses joined by `\n`, stanzas by `\n\n`. Main field for NLP. |
| `raw_wikitext` | Original MediaWiki markup. Useful for editorial analysis. |
| `structure.stanzas` | Nested array: stanzas → verses. |
| `metadata.author` | From HTML microdata or wikitext templates, normalized. |
| `collection_page_id` | Links poem to parent collection page. `null` if unknown. |
| `section_title` | Section within the collection (e.g., "Spleen et Idéal"). |
| `poem_order` | Zero-based position in collection. Reconstructs reading order. |
| `hub_page_id` | Groups multiple editions. Equals own `page_id` if standalone. |
| `collection_structure` | Full structure object (first poem of each collection only). |
| `provenance` | `"api"` (online) or `"dump"` (offline). |
| `checksum_sha256` | SHA-256 of `raw_wikitext`. For deduplication and integrity. |

</details>

<details>
<summary><strong>🏗️ Architecture</strong></summary>

### Shared parsing engine

Both modes use the same modules for classification and extraction:

- **`classifier.py`** — classifies pages as `POEM`, `POETIC_COLLECTION`, `MULTI_VERSION_HUB`, `AUTHOR`, `DISAMBIGUATION`, or `OTHER` using category membership and HTML structure signals.
- **`parsing.py`** — extracts poem structure from rendered HTML (`<div class="poem">` / `<poem>` blocks), splitting into stanzas and verses.
- **`processors.py`** — merges HTML metadata (`itemprop`) with wikitext template metadata (`mwparserfromhell`), normalizes author names, produces validated `PoemSchema`.

**Why HTML, not just wikitext?** Most Wikisource poems use ProofreadPage transclusion (`<pages index="Book.djvu" from=42 to=43 />`). The raw wikitext contains only this directive, not actual verses. Only the rendered HTML has fully resolved poem content.

### Online pipeline

Async Producer-Consumer model:

1. **Producer** crawls the category tree via MediaWiki API.
2. **N consumer tasks** (`--workers`) concurrently fetch wikitext + HTML, classify, and route pages.
3. **Writer thread** handles all sync disk I/O (JSONL + SQLite).

Rate limiting: semaphore, 10-RPS sliding window, exponential backoff, `Retry-After` compliance.

### Offline pipeline

Sequential 5-phase batch pipeline with no network I/O:

```
Phase 1          Phase 2           Phase 3          Phase 4          Phase 5
SQL Dumps ──────> NDJSON Files ───> In-Memory ──────> XML Dumps ─────> Write Output
  → SQLite          → Classify       Enrichment        → Wikitext       → JSONL + DB
    Index           (multiprocess)
```

**Phase 1 — Build index:** Parses SQL dumps into SQLite. Custom state-machine parser handles MySQL `INSERT` syntax and `varbinary` UTF-8 decoding. BFS category-tree traversal finds target page IDs. Uses modern MediaWiki schema (`cl_target_id` → `linktarget` join).

**Phase 2 — Stream NDJSON:** Streams ~21 GB of Enterprise HTML dumps. Regex pre-filter on raw bytes skips irrelevant pages before JSON parsing. Parallel classification via `ProcessPoolExecutor`. Collections and hubs trigger a bounded second pass for newly discovered pages.

**Phase 3 — Enrich:** Maps poems to parent collections and hubs using in-memory data from Phase 2. No network calls.

**Phase 4 — Extract wikitext:** Streams XML dumps with `iterparse` at constant memory. Provides `raw_wikitext` and `checksum_sha256` fields that NDJSON lacks.

> Why both NDJSON *and* XML? NDJSON has rendered HTML (resolved transclusions) but no wikitext. XML has raw wikitext but no rendered HTML. Both are needed.

**Phase 5 — Write:** Processes each poem through `PoemProcessor.process()` (same code as online mode). Output sorted by `page_id` for reproducibility.

### Data reconciliation — the Golden Record

The online and offline pipelines have complementary strengths and weaknesses. Neither dataset alone is the ground truth.

The **`merger.py`** module reconciles two JSONL outputs into a single "Golden Record" corpus. It streams both files, deduplicates by `page_id`, and resolves conflicts using a configurable strategy. For the `keep_richest` strategy, it applies a **deterministic richness scoring algorithm** to each duplicate pair:

| Signal | Points | Rationale |
|---|---|---|
| `collection_page_id` is present | **+50** | Being linked to a parent collection is the highest-value contextual data. |
| `hub_page_id` ≠ `page_id` | **+30** | Proper grouping under a multi-version hub adds scholarly context. |
| `section_title` is present | **+20** | Section placement within a collection aids reading-order reconstruction. |
| Each non-empty metadata field | **+10** | One bonus per filled field among `author`, `publication_date`, `source_collection`, `publisher`, `translator`. |
| Text length | **+1 per 100 chars** | Tie-breaker: longer `normalized_text` suggests a more complete extraction. |

When scores are tied, the version from file A is kept. The merger logs a full summary — total records per file, duplicates found, conflict outcomes, and elapsed time — so you can audit every merge.

</details>

<details>
<summary><strong>📋 Full CLI Reference</strong></summary>

After installation, the tool is available as `wikisourcescraper` or `python -m wikisource_scraper`.

### Scraping

```bash
# Online (default)
wikisourcescraper scrape --lang fr --category "Poèmes par Auteur" --workers 15 --tree-log

# Offline
wikisourcescraper scrape --lang fr --category "Poèmes" --mode offline --dumps-dir ./dumps --workers 8

# Resume an interrupted run (either mode)
wikisourcescraper scrape --lang fr --category "Poèmes" --resume
```

### Scrape options

| Option | Default | Description |
|---|---|---|
| `--lang` | *(required)* | Wikisource language code (`fr`, `en`, etc.) |
| `--category` | *(required)* | Root category to start from |
| `--output_dir` | `./data/` | Output directory |
| `--workers` | `3` | Online: concurrent requests. Offline: CPU cores. |
| `--limit` | `None` | Process at most N pages (testing) |
| `--resume` | `false` | Skip already-processed pages |
| `--tree-log` | `false` | Write per-author exploration tree logs |
| `--mode` | `online` | `online` or `offline` |
| `--dumps-dir` | `None` | Path to dump files (required for offline) |
| `--bot-user` | `$WIKISOURCE_BOT_USERNAME` | Bot username (online only) |
| `--bot-pass` | `$WIKISOURCE_BOT_PASSWORD` | Bot password (online only) |

### Post-processing

```bash
# Clean: deduplicate by page_id, normalize titles
wikisourcescraper clean -i data/poems.jsonl.gz -o data/poems.cleaned.jsonl.gz

# Enrich: fill missing collection_page_id via API
wikisourcescraper enrich -i data/poems.cleaned.jsonl.gz -o data/poems.enriched.jsonl.gz --lang fr

# Analyze: print corpus statistics
wikisourcescraper analyze data/poems.cleaned.jsonl.gz

# Debug: extract poems with unidentified collections
wikisourcescraper debug -i data/poems.enriched.jsonl.gz -o data/debug.unidentified.jsonl.gz
```

### Merge: reconcile two datasets

Combine an offline and an online corpus (or any two JSONL files) into a single deduplicated Golden Record:

```bash
wikisourcescraper merge \
  --file-a data/offline_corpus.jsonl.gz \
  --file-b data/online_corpus.jsonl.gz \
  --output data/golden_record.jsonl.gz \
  --strategy keep_richest
```

| Option | Default | Description |
|---|---|---|
| `--file-a` | *(required)* | First input file (`.jsonl` or `.jsonl.gz`) |
| `--file-b` | *(required)* | Second input file (`.jsonl` or `.jsonl.gz`) |
| `--output`, `-o` | *(required)* | Merged output file |
| `--strategy` | `keep_richest` | Conflict resolution when a `page_id` exists in both files |

**Strategies:**

- **`keep_richest`** *(default)* — scores both versions of a duplicate poem and keeps the semantically richer one. Ties go to file A. Best for building the most complete corpus possible.
- **`keep_a`** — always keeps file A's version on conflict. Useful when A is your trusted reference (e.g., a carefully curated offline dump).
- **`keep_b`** — always keeps file B's version on conflict.

</details>

---

## License

MIT. See [`LICENSE`](LICENSE).
