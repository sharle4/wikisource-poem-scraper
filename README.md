# Wikisource Poem Scraper v5.0.0

A Python tool that extracts, classifies, and structures poetry from Wikisource (primarily fr.wikisource.org) into clean, validated JSONL files.

It works in two modes:

- **Online mode** queries the MediaWiki API and fetches live rendered HTML. Good for incremental or targeted runs.
- **Offline mode** processes local Wikimedia dump files (SQL, XML, NDJSON) entirely locally. Good for building the full corpus without hitting any API.

Both modes share the same classification and parsing logic and produce the same output schema. The only difference in the output is the `provenance` field (`"api"` vs `"dump"`).

> **Data license:** The source code is MIT-licensed. The extracted data is subject to Wikisource's own licenses (typically Public Domain or CC-BY-SA). Respect them.

---

<details>
<summary><strong>Setting Up a Bot Account (Online Mode)</strong></summary>

### Why bother?

Anonymous API requests are heavily rate-limited. Sustained scraping without authentication risks HTTP 429 errors or IP bans. A bot account gives you higher rate limits and signals to Wikimedia that you are a legitimate automated client.

### How to create one

1. Log in to your Wikisource account (e.g., on `fr.wikisource.org`). Create one if you don't have one.
2. Go to **Special:BotPasswords** (`https://fr.wikisource.org/wiki/Special:BotPasswords`).
3. Enter a bot name (e.g., `PoemScraper`). Your login name will be `YourUsername@PoemScraper`.
4. Grant at least **Basic rights** and **High-volume editing**. Click **Create**.
5. Save the generated password. It is shown only once.

### Configure the scraper

Create a `.env` file at the project root:

```env
WIKISOURCE_BOT_USERNAME=YourUsername@PoemScraper
WIKISOURCE_BOT_PASSWORD=the_generated_password
```

The scraper loads these automatically. You can also pass `--bot-user` and `--bot-pass` on the command line.

</details>

<details>
<summary><strong>Installation & Prerequisites</strong></summary>

### Python

Requires **Python 3.10+** (tested up to 3.12).

### Install

```bash
git clone https://github.com/sharle4/wikisource-poem-scraper.git
cd wikisource-poem-scraper
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows
pip install -e .
```

For development tools (pytest, black, mypy):

```bash
pip install -e ".[dev]"
```

### Dump files for offline mode

Offline mode needs three types of dump files, all placed in a single directory:

**SQL dumps** and **XML dumps** are freely available from `https://dumps.wikimedia.org/frwikisource/latest/`:

| File | Size | Content |
|---|---|---|
| `frwikisource-latest-page.sql` | ~726 MB | Page metadata |
| `frwikisource-latest-categorylinks.sql` | ~743 MB | Category membership |
| `frwikisource-latest-linktarget.sql` | ~887 MB | Category name resolution |
| `frwikisource-latest-redirect.sql` | ~4.5 MB | Redirects |
| `frwikisource-latest-pages-articles1.xml-*` (4 files) | ~11.4 GB | Raw wikitext for all pages |

**HTML NDJSON dumps** require a free Wikimedia Enterprise account:

1. Create an account at `https://enterprise.wikimedia.com/`.
2. Use the provided PowerShell script to download and extract the dumps:

   ```powershell
   # Edit your credentials in the script first
   .\dumps\download_enterprise_dumps.ps1
   ```

   This downloads `frwikisource_namespace_0_*.ndjson` (10 files, ~21 GB total) containing the fully rendered HTML for every namespace-0 page.

**Total disk space needed:** ~35 GB for dumps, plus 2-4 GB for the generated index.

</details>

<details>
<summary><strong>CLI Usage Guide</strong></summary>

After installation, the tool is available as `poemscraper` or `python -m poemscraper`.

### Scraping

**Online mode (default):**

```bash
poemscraper scrape --lang fr --category "Poèmes par Auteur" --workers 15 --tree-log
```

**Offline mode:**

```bash
poemscraper scrape --lang fr --category "Poèmes" --mode offline --dumps-dir ./dumps --workers 8
```

**Resume an interrupted run** (works in both modes):

```bash
poemscraper scrape --lang fr --category "Poèmes" --mode offline --dumps-dir ./dumps --resume
```

### Scrape options

| Option | Default | Description |
|---|---|---|
| `--lang` | *(required)* | Wikisource language code (`fr`, `en`, etc.) |
| `--category` | *(required)* | Root category to start from |
| `--output_dir` | `./data/` | Where to write output files |
| `--workers` | `3` | Online: concurrent HTTP requests. Offline: CPU cores for multiprocessing. |
| `--limit` | `None` | Process at most N pages (for testing) |
| `--resume` | `false` | Skip already-processed pages using the SQLite index |
| `--tree-log` | `false` | Write per-author exploration tree logs |
| `--mode` | `online` | `online` or `offline` |
| `--dumps-dir` | `None` | Path to dump files (required for offline) |
| `--bot-user` | `$WIKISOURCE_BOT_USERNAME` | Bot username (online only) |
| `--bot-pass` | `$WIKISOURCE_BOT_PASSWORD` | Bot password (online only) |

### Post-processing commands

```bash
# Enrich: fill in missing collection_page_id values via the API (online mode output only)
poemscraper enrich -i data/poems.cleaned.jsonl.gz -o data/poems.enriched.jsonl.gz --lang fr

# Clean: deduplicate by page_id, normalize titles
poemscraper clean -i data/poems.jsonl.gz -o data/poems.cleaned.jsonl.gz

# Analyze: print corpus statistics (author counts, collection sizes, verse counts, etc.)
poemscraper analyze data/poems.cleaned.jsonl.gz

# Debug: extract poems with a collection_title but no collection_page_id
poemscraper debug -i data/poems.enriched.jsonl.gz -o data/debug.unidentified.jsonl.gz
```

</details>

<details>
<summary><strong>Output Data Structure</strong></summary>

### Output files

Each run produces three files in `--output_dir`:

| File | Description |
|---|---|
| `poems.jsonl.gz` | One JSON object per line, per extracted poem (may contain duplicates in online mode) |
| `poems.cleaned.jsonl.gz` | Deduplicated, title-normalized version. One unique poem per `page_id`. |
| `poems_index.sqlite` | Lightweight SQLite index for fast lookups and resume support |

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

### Key fields explained

| Field | What it is |
|---|---|
| `normalized_text` | Flat text of the poem. Verses joined by `\n`, stanzas by `\n\n`. Main field for NLP. |
| `raw_wikitext` | Original MediaWiki markup of the page revision. Useful for editorial analysis. |
| `structure.stanzas` | Nested array: list of stanzas, each a list of verse strings. |
| `metadata.author` | Author name, extracted from HTML microdata or wikitext templates, then normalized. |
| `collection_page_id` | Links this poem to its parent collection's MediaWiki page. `null` if unknown. |
| `section_title` | Section name within the collection (e.g., "Spleen et Ideal"). |
| `poem_order` | Zero-based position in the collection. Allows reconstructing reading order. |
| `hub_page_id` | Groups multiple editions of the same poem. Equals own `page_id` if standalone. |
| `collection_structure` | Full collection structure object. Only present on the *first* poem of each collection to avoid duplication. |
| `provenance` | `"api"` (online mode) or `"dump"` (offline mode). |
| `checksum_sha256` | SHA-256 of `raw_wikitext`. For deduplication and integrity. |

</details>

<details>
<summary><strong>Architecture & Inner Workings</strong></summary>

### The shared parsing brain

Both modes use the exact same modules for classification and extraction:

- **`classifier.py`** classifies each page as `POEM`, `POETIC_COLLECTION`, `MULTI_VERSION_HUB`, `AUTHOR`, `DISAMBIGUATION`, or `OTHER` using category membership and HTML structure signals (presence of `<div class="poem">`, table of contents, Wikidata links, etc.).
- **`parsing.py`** extracts poem structure from rendered HTML by finding `<div class="poem">` or `<poem>` blocks, splitting text into stanzas and verses.
- **`processors.py`** merges HTML metadata (`itemprop` attributes) with wikitext template metadata (`mwparserfromhell`), normalizes author names, and produces the validated `PoemSchema`.

**Why parse HTML instead of just wikitext?** On Wikisource, most poem text is not inline in the article's wikitext. Instead, articles use ProofreadPage transclusion directives like `<pages index="Book.djvu" from=42 to=43 />`, which pull text from separate scan pages. The raw wikitext contains only this directive, not the actual verses. Only the rendered HTML (from the live site or the NDJSON dumps) has the fully resolved poem content. The raw wikitext is still used for template-based metadata and the `checksum_sha256` field.

### Online pipeline (network-bound)

Uses an async Producer-Consumer model:

1. **Producer** crawls the category tree via the MediaWiki API, discovering pages.
2. **N consumer tasks** (set by `--workers`) concurrently fetch each page's wikitext (API) and rendered HTML (live site), classify the page, and route it:
   - `POEM` pages are processed and sent to the writer.
   - `POETIC_COLLECTION` pages have their ordered poem links extracted and re-queued with collection context.
   - `MULTI_VERSION_HUB` pages have their sub-versions extracted and re-queued with hub context.
3. **Writer thread** handles all synchronous disk I/O (JSONL + SQLite) on a separate thread.

Rate limiting is enforced via a semaphore, a 10-RPS sliding window, exponential backoff on transient errors, and `Retry-After` header compliance on 429 responses.

### Offline pipeline (CPU/IO-bound)

A sequential 5-phase batch pipeline with no network I/O:

```
Phase 1          Phase 2           Phase 3          Phase 4          Phase 5
SQL Dumps ──────> NDJSON Files ───> In-Memory ──────> XML Dumps ─────> Write Output
  → SQLite          → Classify       Enrichment        → Wikitext       → JSONL + DB
    Index           (multiprocess)
```

**Phase 1 -- Build index:** Parses the 4 SQL dump files into a local SQLite database. A custom state-machine parser handles MySQL `INSERT` syntax and decodes `varbinary` UTF-8 fields. Then performs a BFS traversal of the category tree to identify target page IDs.

> Modern MediaWiki stores category membership using a numeric `cl_target_id` foreign key to the `linktarget` table, not a text `cl_to` column. This is why the `linktarget.sql` dump is required -- you must join through it to resolve category names.

**Phase 2 -- Stream NDJSON:** Streams the ~21 GB of Enterprise HTML dumps. A regex pre-filter extracts each line's `"identifier"` from the raw bytes before doing a full JSON parse, skipping irrelevant pages. Matching pages are classified in parallel using `ProcessPoolExecutor`. Collections and hubs trigger a bounded second pass for any newly discovered poem pages.

**Phase 3 -- Enrich:** Maps poems to their parent collections and hubs using the data gathered in Phase 2. No network calls needed -- the title-to-page_id mapping comes from the SQLite index.

**Phase 4 -- Extract wikitext:** Streams the XML dumps using `iterparse` with constant memory. Only pages in the pending set are kept. This provides the `raw_wikitext` and `checksum_sha256` fields that the NDJSON dumps lack.

> Why both NDJSON *and* XML? NDJSON has the rendered HTML (with resolved transclusions) but no wikitext. XML has the raw wikitext but no rendered HTML. Both are needed.

**Phase 5 -- Write:** Each pending poem gets its wikitext injected, its HTML re-parsed, and is processed through `PoemProcessor.process()` (the same code as online mode). Output is written sorted by `page_id` for reproducibility.

</details>

---

## License

MIT. See `LICENSE`.
