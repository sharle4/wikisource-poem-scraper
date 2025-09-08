import hashlib
import logging
from datetime import datetime, timezone

import mwparserfromhell

from .schemas import PoemSchema, PoemStructure
from .parsing import WikitextParser
from .exceptions import PoemParsingError

logger = logging.getLogger(__name__)

class PoemProcessor:
    """
    Transforms raw MediaWiki page data into a clean, validated PoemSchema object.
    """
    def process(self, page_data: dict, lang: str) -> PoemSchema:
        """
        Main processing method for a single page's data.
        
        Args:
            page_data: The JSON dictionary for a page from the MediaWiki API response.
            lang: The language code of the Wikisource.

        Returns:
            A validated PoemSchema instance.

        Raises:
            PoemParsingError: If the page does not contain a valid poem structure.
        """
        wikitext = page_data['revisions'][0]['content']
        page_id = page_data['pageid']
        title = page_data['title']
        
        structure = WikitextParser.extract_poem_structure(wikitext)
        if not structure:
            raise PoemParsingError("No <poem> tags found or content is empty.")
            
        parsed_wikicode = mwparserfromhell.parse(wikitext)
        
        metadata = self._extract_metadata(parsed_wikicode)
        author = metadata.get("author")
        
        normalized_text = WikitextParser.create_normalized_text(structure)

        poem_obj = PoemSchema(
            page_id=page_id,
            revision_id=page_data['revisions'][0]['revid'],
            title=title,
            author=author,
            author_url=f"https://{lang}.wikisource.org/wiki/Auteur:{author.replace(' ', '_')}" if author else None,
            language=lang,
            wikisource_url=page_data.get('fullurl', f"https://{lang}.wikisource.org/?curid={page_id}"),
            license={"name": "License not automatically detected", "url": None},
            metadata=metadata,
            raw_wikitext=wikitext,
            structure=structure,
            normalized_text=normalized_text,
            checksum_sha256=hashlib.sha256(wikitext.encode('utf-8')).hexdigest(),
            extraction_timestamp=datetime.now(timezone.utc),
            provenance="api"
        )
        return poem_obj

    def _extract_metadata(self, parsed_wikicode: mwparserfromhell.wikicode.Wikicode) -> dict:
        """
        Extracts metadata like author, publication date, and categories.
        """
        metadata = {"categories": [], "templates": {}}

        for link in parsed_wikicode.filter_wikilinks():
            if link.title.startswith("Catégorie:"):
                cat_name = str(link.title).split(":", 1)[1]
                metadata["categories"].append(cat_name)

        for template in parsed_wikicode.filter_templates():
            template_name = str(template.name).strip()
            params = {}
            for param in template.params:
                params[str(param.name).strip()] = str(param.value).strip()
            metadata["templates"][template_name] = params
            
            if template_name.lower() in ["auteur", "author"]:
                if '1' in params and params['1']:
                    metadata['author'] = params['1']

        return metadata