"""
Streaming readers for Wikimedia dump files.

Provides memory-efficient iterators for:
- SQL dump INSERT statements (page, categorylinks, linktarget, redirect)
- NDJSON Enterprise HTML dumps (namespace 0)
- XML article dumps (all namespaces)
"""
import json
import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterator, List, Optional, Set, Tuple

from tqdm import tqdm

logger = logging.getLogger(__name__)

# MediaWiki XML namespace
MW_NS = "{http://www.mediawiki.org/xml/export-0.11/}"

# Pre-compiled regex for fast page_id extraction from NDJSON lines
_IDENTIFIER_RE = re.compile(rb'"identifier"\s*:\s*(\d+)')


# ---------------------------------------------------------------------------
# SQL dump parser
# ---------------------------------------------------------------------------

def _parse_sql_values(line: str) -> Iterator[List]:
    """
    Parse a single INSERT INTO ... VALUES (...),(...); line.

    Uses a character-by-character state machine to correctly handle:
    - Single-quoted strings with MySQL escaping (\\, \', \")
    - NULL values
    - Integer values
    - Nested parentheses (value tuples)

    Yields one list of parsed Python values per tuple.
    """
    # Find the start of VALUES
    values_idx = line.find("VALUES ")
    if values_idx == -1:
        return
    pos = values_idx + 7  # skip "VALUES "
    length = len(line)

    while pos < length:
        # Skip to opening paren
        while pos < length and line[pos] != '(':
            pos += 1
        if pos >= length:
            return
        pos += 1  # skip '('

        values: List = []
        while pos < length:
            ch = line[pos]

            if ch == ')':
                pos += 1
                yield values
                break

            if ch == ',':
                pos += 1
                continue

            if ch == ' ':
                pos += 1
                continue

            # NULL
            if ch == 'N' and line[pos:pos+4] == 'NULL':
                values.append(None)
                pos += 4
                continue

            # Quoted string
            if ch == "'":
                pos += 1
                parts = []
                while pos < length:
                    c = line[pos]
                    if c == '\\':
                        pos += 1
                        if pos < length:
                            escaped = line[pos]
                            if escaped == "'":
                                parts.append("'")
                            elif escaped == '\\':
                                parts.append('\\')
                            elif escaped == '"':
                                parts.append('"')
                            elif escaped == 'n':
                                parts.append('\n')
                            elif escaped == 'r':
                                parts.append('\r')
                            elif escaped == 't':
                                parts.append('\t')
                            elif escaped == '0':
                                parts.append('\0')
                            else:
                                parts.append(escaped)
                        pos += 1
                        continue
                    if c == "'":
                        pos += 1
                        break
                    parts.append(c)
                    pos += 1
                raw_str = "".join(parts)
                # Decode binary varbinary fields: the SQL file stores raw
                # UTF-8 bytes encoded as latin-1 characters in the string.
                try:
                    decoded = raw_str.encode("latin-1").decode("utf-8")
                except (UnicodeDecodeError, UnicodeEncodeError):
                    decoded = raw_str
                values.append(decoded)
                continue

            # Number (integer or float, possibly negative)
            if ch.isdigit() or ch == '-' or ch == '.':
                start = pos
                pos += 1
                while pos < length and (line[pos].isdigit() or line[pos] == '.' or line[pos] == 'e' or line[pos] == 'E' or line[pos] == '+' or line[pos] == '-'):
                    if line[pos] in '+-' and pos > start + 1 and line[pos-1] not in 'eE':
                        break
                    pos += 1
                num_str = line[start:pos]
                if '.' in num_str or 'e' in num_str or 'E' in num_str:
                    values.append(float(num_str))
                else:
                    values.append(int(num_str))
                continue

            # Unknown character — skip
            pos += 1


def _iter_sql_inserts(sql_path: Path, desc: str) -> Iterator[List]:
    """
    Streams through a SQL dump file, yielding parsed value tuples
    from INSERT INTO statements.
    """
    file_size = sql_path.stat().st_size
    bytes_read = 0
    with tqdm(total=file_size, desc=desc, unit="B", unit_scale=True, unit_divisor=1024) as pbar:
        with open(sql_path, "r", encoding="latin-1", errors="replace") as f:
            for line in f:
                bytes_read += len(line.encode("latin-1", errors="replace"))
                pbar.update(len(line.encode("latin-1", errors="replace")))
                if not line.startswith("INSERT INTO"):
                    continue
                yield from _parse_sql_values(line)


def iter_page_rows(sql_path: Path) -> Iterator[Tuple[int, int, str, int, int]]:
    """
    Yields (page_id, page_namespace, page_title, page_is_redirect, page_len)
    from page.sql dump.
    page_title is decoded from binary to UTF-8, underscores replaced with spaces.
    """
    for row in _iter_sql_inserts(sql_path, "Parsing page.sql"):
        # Value order: (page_id, page_namespace, page_title, page_is_redirect,
        #               page_is_new, page_random, page_touched, page_links_updated,
        #               page_latest, page_len, page_content_model, page_lang)
        try:
            page_id = int(row[0])
            ns = int(row[1])
            title = str(row[2]).replace("_", " ")
            is_redirect = int(row[3])
            page_len = int(row[9])
            yield page_id, ns, title, is_redirect, page_len
        except (IndexError, ValueError, TypeError) as e:
            logger.debug(f"Skipping malformed page row: {e}")
            continue


def iter_categorylinks_rows(sql_path: Path) -> Iterator[Tuple[int, str, int]]:
    """
    Yields (cl_from, cl_type, cl_target_id) from categorylinks.sql dump.
    cl_type is one of: 'page', 'subcat', 'file'.
    """
    for row in _iter_sql_inserts(sql_path, "Parsing categorylinks.sql"):
        # Value order: (cl_from, cl_sortkey, cl_timestamp, cl_sortkey_prefix,
        #               cl_type, cl_collation_id, cl_target_id)
        try:
            cl_from = int(row[0])
            cl_type = str(row[4])
            cl_target_id = int(row[6])
            yield cl_from, cl_type, cl_target_id
        except (IndexError, ValueError, TypeError) as e:
            logger.debug(f"Skipping malformed categorylinks row: {e}")
            continue


def iter_linktarget_rows(sql_path: Path) -> Iterator[Tuple[int, int, str]]:
    """
    Yields (lt_id, lt_namespace, lt_title) from linktarget.sql dump.
    lt_title is decoded from binary to UTF-8.
    """
    for row in _iter_sql_inserts(sql_path, "Parsing linktarget.sql"):
        # Value order: (lt_id, lt_namespace, lt_title)
        try:
            lt_id = int(row[0])
            lt_namespace = int(row[1])
            lt_title = str(row[2])
            yield lt_id, lt_namespace, lt_title
        except (IndexError, ValueError, TypeError) as e:
            logger.debug(f"Skipping malformed linktarget row: {e}")
            continue


def iter_redirect_rows(sql_path: Path) -> Iterator[Tuple[int, int, str]]:
    """
    Yields (rd_from, rd_namespace, rd_title) from redirect.sql dump.
    rd_title is decoded from binary to UTF-8.
    """
    for row in _iter_sql_inserts(sql_path, "Parsing redirect.sql"):
        # Value order: (rd_from, rd_namespace, rd_title, rd_interwiki, rd_fragment)
        try:
            rd_from = int(row[0])
            rd_namespace = int(row[1])
            rd_title = str(row[2])
            yield rd_from, rd_namespace, rd_title
        except (IndexError, ValueError, TypeError) as e:
            logger.debug(f"Skipping malformed redirect row: {e}")
            continue


# ---------------------------------------------------------------------------
# NDJSON Enterprise HTML dump streamer
# ---------------------------------------------------------------------------

def iter_ndjson_pages(
    ndjson_dir: Path,
    target_page_ids: Optional[Set[int]] = None,
) -> Iterator[dict]:
    """
    Streams through all frwikisource_namespace_0_*.ndjson files in sorted order.

    For each line, parses JSON and yields a dict with:
    {
        "page_id": int,
        "revision_id": int,
        "title": str,
        "url": str,
        "html": str,
        "date_modified": str,
    }

    If target_page_ids is provided, only yields pages whose identifier is in the set.
    Uses a regex pre-filter on the raw bytes to skip full JSON parsing for irrelevant pages.
    """
    ndjson_files = sorted(ndjson_dir.glob("frwikisource_namespace_0_*.ndjson"))
    if not ndjson_files:
        logger.error(f"No NDJSON files found in {ndjson_dir}")
        return

    total_size = sum(f.stat().st_size for f in ndjson_files)

    with tqdm(total=total_size, desc="Streaming NDJSON", unit="B", unit_scale=True, unit_divisor=1024) as pbar:
        for ndjson_file in ndjson_files:
            logger.info(f"Processing NDJSON file: {ndjson_file.name}")
            with open(ndjson_file, "rb") as f:
                for line_num, raw_line in enumerate(f, 1):
                    pbar.update(len(raw_line))

                    if not raw_line.strip():
                        continue

                    # Fast pre-filter: extract identifier from raw bytes before full JSON parse
                    if target_page_ids is not None:
                        match = _IDENTIFIER_RE.search(raw_line, 0, min(len(raw_line), 500))
                        if match:
                            page_id = int(match.group(1))
                            if page_id not in target_page_ids:
                                continue
                        else:
                            # Can't extract identifier quickly — fall through to full parse
                            pass

                    try:
                        record = json.loads(raw_line)
                    except json.JSONDecodeError as e:
                        logger.warning(
                            f"Malformed JSON at {ndjson_file.name}:{line_num}: {e}"
                        )
                        continue

                    page_id = record.get("identifier")
                    if page_id is None:
                        continue

                    if target_page_ids is not None and page_id not in target_page_ids:
                        continue

                    article_body = record.get("article_body", {})
                    html = article_body.get("html", "")

                    yield {
                        "page_id": page_id,
                        "revision_id": record.get("version", {}).get("identifier", 0),
                        "title": record.get("name", ""),
                        "url": record.get("url", ""),
                        "html": html,
                        "date_modified": record.get("date_modified", ""),
                    }


# ---------------------------------------------------------------------------
# XML article dump streamer
# ---------------------------------------------------------------------------

def iter_xml_pages(
    xml_dir: Path,
    target_page_ids: Optional[Set[int]] = None,
    target_namespace: Optional[int] = 0,
) -> Iterator[dict]:
    """
    Streams through all frwikisource-latest-pages-articles1.xml-* files
    using iterparse.

    For each <page> element, yields:
    {
        "page_id": int,
        "title": str,
        "namespace": int,
        "revision_id": int,
        "wikitext": str,
    }

    If target_page_ids is provided, only yields pages whose id is in the set.
    If target_namespace is provided, only yields pages in that namespace.
    Uses iterparse with element clearing to maintain constant memory usage.
    """
    xml_files = sorted(xml_dir.glob("frwikisource-latest-pages-articles1.xml-*"))
    if not xml_files:
        logger.error(f"No XML dump files found in {xml_dir}")
        return

    total_size = sum(f.stat().st_size for f in xml_files)
    yielded = 0

    with tqdm(total=total_size, desc="Streaming XML dumps", unit="B", unit_scale=True, unit_divisor=1024) as pbar:
        for xml_file in xml_files:
            logger.info(f"Processing XML file: {xml_file.name}")
            file_size_before = 0

            try:
                context = ET.iterparse(str(xml_file), events=("end",))
                # We need the root element to clear it periodically
                root = None

                for event, elem in context:
                    if root is None:
                        # Walk up to find root
                        root = elem

                    tag = elem.tag

                    # Only process <page> elements
                    if tag != f"{MW_NS}page" and tag != "page":
                        continue

                    try:
                        # Extract fields — try with and without namespace prefix
                        ns_elem = elem.find(f"{MW_NS}ns")
                        if ns_elem is None:
                            ns_elem = elem.find("ns")
                        namespace = int(ns_elem.text) if ns_elem is not None and ns_elem.text else 0

                        if target_namespace is not None and namespace != target_namespace:
                            elem.clear()
                            continue

                        id_elem = elem.find(f"{MW_NS}id")
                        if id_elem is None:
                            id_elem = elem.find("id")
                        page_id = int(id_elem.text) if id_elem is not None and id_elem.text else None

                        if page_id is None:
                            elem.clear()
                            continue

                        if target_page_ids is not None and page_id not in target_page_ids:
                            elem.clear()
                            continue

                        title_elem = elem.find(f"{MW_NS}title")
                        if title_elem is None:
                            title_elem = elem.find("title")
                        title = title_elem.text if title_elem is not None else ""

                        # Get revision data
                        revision = elem.find(f"{MW_NS}revision")
                        if revision is None:
                            revision = elem.find("revision")

                        revision_id = 0
                        wikitext = ""
                        if revision is not None:
                            rev_id_elem = revision.find(f"{MW_NS}id")
                            if rev_id_elem is None:
                                rev_id_elem = revision.find("id")
                            revision_id = int(rev_id_elem.text) if rev_id_elem is not None and rev_id_elem.text else 0

                            text_elem = revision.find(f"{MW_NS}text")
                            if text_elem is None:
                                text_elem = revision.find("text")
                            wikitext = text_elem.text if text_elem is not None and text_elem.text else ""

                        yield {
                            "page_id": page_id,
                            "title": title or "",
                            "namespace": namespace,
                            "revision_id": revision_id,
                            "wikitext": wikitext,
                        }
                        yielded += 1

                    finally:
                        elem.clear()

                # Update progress for this file
                pbar.update(xml_file.stat().st_size)

            except ET.ParseError as e:
                logger.error(f"XML parse error in {xml_file.name}: {e}")
                pbar.update(xml_file.stat().st_size)
                continue

    logger.info(f"XML streaming complete. Yielded {yielded} pages.")
