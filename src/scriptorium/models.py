import datetime
from typing import List, Optional, Literal, Union
from pydantic import BaseModel, Field, HttpUrl

class BaseSchema(BaseModel):
    """Base class for all models with common configurations."""
    class Config:
        orm_mode = True

class Author(BaseSchema):
    id: int = Field(..., description="Unique author category ID.")
    name: str = Field(..., description="Author name.")
    wikisource_url: HttpUrl = Field(..., description="URL of the author's category page.")

class PoeticCollection(BaseSchema):
    id: int = Field(..., description="Collection page ID.")
    title: str = Field(..., description="Collection title.")
    author_id: int = Field(..., description="Foreign key to the author.")
    wikisource_url: HttpUrl = Field(..., description="URL of the collection page.")

class VersionHub(BaseSchema):
    """Represents a page that serves as a portal to multiple versions of the same poem."""
    id: int = Field(..., description="Version portal page ID.")
    title: str = Field(..., description="Poem title (without the version).")
    author_id: int = Field(..., description="Foreign key to the author.")

    collection_id: Optional[int] = Field(None, description="Optional foreign key to the collection.")
    wikisource_url: HttpUrl = Field(..., description="URL of the portal page.")


class PoemStructure(BaseSchema):
    """Normalized poem structure (stanzas and verses)."""
    stanzas: List[List[str]] = Field(..., description="List of stanzas, each containing a list of verses.")
    raw_markers: List[str] = Field(default_factory=list, description="Raw HTML markers detected for poem blocks.")

class PoemMetadata(BaseSchema):
    """Structured container for all extracted metadata."""
    author_name: Optional[str] = Field(None, description="Primary author(s) of the poem (extracted from the page).")
    publication_date: Optional[str] = Field(None, description="Publication date (often just the year).")
    source_collection_name: Optional[str] = Field(None, description="Name of the original collection or publication (extracted from the page).")
    publisher: Optional[str] = Field(None, description="Publishing house.")
    translator: Optional[str] = Field(None, description="Translator, if applicable.")
    license_name: Optional[str] = Field(None, description="Detected license name.")

class Poem(BaseSchema):
    """Complete and validated JSON schema for a single poem."""
    page_id: int = Field(..., description="Unique MediaWiki page identifier (pageid).")
    revision_id: int = Field(..., description="Unique identifier of the specific extracted revision (revid).")
    title: str = Field(..., description="Canonical page title (poem).")
    language: str = Field(..., description="Language code of the Wikisource project (e.g., 'fr').")
    wikisource_url: HttpUrl = Field(..., description="Full canonical URL to the poem page.")

    author_id: int = Field(..., description="Foreign key to the author.")
    collection_id: Optional[int] = Field(None, description="Foreign key to the parent collection.")
    hub_id: Optional[int] = Field(None, description="Foreign key to the parent version portal.")

    metadata: PoemMetadata = Field(..., description="All extracted metadata.")
    structure: PoemStructure = Field(..., description="Parsed poem structure.")
    normalized_text: str = Field(..., description="Full poem text, cleaned and concatenated.")
    raw_wikitext: str = Field(..., description="Complete raw wikitext content of the revision.")
    checksum_sha256: str = Field(..., description="SHA-256 of the 'raw_wikitext' field for deduplication and integrity.")
    extraction_timestamp: datetime.datetime = Field(..., description="ISO 8601 (UTC) timestamp of the extraction.")

ScrapedData = Union[Author, PoeticCollection, VersionHub, Poem]
