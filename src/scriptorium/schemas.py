import datetime
from typing import List, Optional, Literal, Union
from pydantic import BaseModel, Field, HttpUrl, field_validator


class PoemInfo(BaseModel):
    """Represents a single poem within a collection."""
    title: str
    page_id: int
    url: HttpUrl


class Section(BaseModel):
    """Represents a titled section within a collection."""
    title: str
    poems: List[PoemInfo] = Field(default_factory=list)


CollectionComponent = Union[Section, PoemInfo]


class Collection(BaseModel):
    """Models the complete structure of a poetry collection."""
    page_id: int
    title: str
    author: Optional[str] = None
    url: HttpUrl
    content: List[CollectionComponent] = Field(default_factory=list)


class PoemStructure(BaseModel):
    """Normalized poem structure (stanzas and verses)."""
    stanzas: List[List[str]] = Field(
        ..., description="List of stanzas, each containing a list of verses."
    )
    raw_markers: List[str] = Field(
        default_factory=list,
        description="Raw HTML markers detected for poem blocks.",
    )


class PoemMetadata(BaseModel):
    """Structured container for all extracted metadata."""
    author: Optional[str] = Field(
        None, description="Primary author(s) of the poem."
    )
    publication_date: Optional[str] = Field(
        None, description="Publication date (often just the year)."
    )
    source_collection: Optional[str] = Field(
        None, description="Name of the original collection or publication."
    )
    publisher: Optional[str] = Field(None, description="Publishing house.")
    translator: Optional[str] = Field(None, description="Translator, if applicable.")


class PoemSchema(BaseModel):
    """
    Complete and validated JSON schema for a single poem, enriched
    with structural information from the parent collection.
    """
    page_id: int = Field(
        ..., description="Unique MediaWiki page identifier (pageid)."
    )
    revision_id: int = Field(
        ..., description="Unique identifier of the specific extracted revision (revid)."
    )
    title: str = Field(..., description="Canonical page title (poem).")
    language: str = Field(
        ..., description="Language code of the Wikisource project (e.g., 'fr')."
    )
    wikisource_url: HttpUrl = Field(
        ..., description="Full canonical URL to the poem page."
    )

    collection_page_id: Optional[int] = Field(
        None, description="Page ID of the parent collection."
    )
    collection_title: Optional[str] = Field(
        None, description="Title of the parent collection."
    )
    section_title: Optional[str] = Field(
        None, description="Title of the poem's section within the collection."
    )
    poem_order: Optional[int] = Field(
        None, description="Ordinal position of the poem within the collection (0-indexed)."
    )
    hub_title: Optional[str] = Field(
        None, description="Title of the parent multi-version hub page."
    )
    hub_page_id: int = Field(
        ..., description="Unique poem group ID. This is the page_id of the parent hub, or the poem's own page_id if standalone."
    )

    metadata: PoemMetadata = Field(..., description="All extracted metadata.")
    structure: PoemStructure = Field(..., description="Parsed poem structure.")
    collection_structure: Optional[Collection] = Field(
        None, description="Complete object describing the parent collection structure (present only for the first poem in a collection to avoid massive data duplication)."
    )
    normalized_text: str = Field(
        ..., description="Full poem text, cleaned and concatenated."
    )
    raw_wikitext: str = Field(
        ..., description="Complete raw wikitext content of the revision."
    )

    checksum_sha256: str = Field(
        ...,
        description="SHA-256 of the 'raw_wikitext' field for deduplication and integrity.",
    )
    extraction_timestamp: datetime.datetime = Field(
        ..., description="ISO 8601 (UTC) timestamp of the extraction."
    )
    provenance: Literal["api", "dump"] = Field(
        "api", description="Data source."
    )

    @field_validator("extraction_timestamp", mode="before")
    @classmethod
    def set_default_timestamp(cls, v):
        """Ensures the timestamp is generated if not provided."""
        if v is None:
            return datetime.datetime.now(datetime.timezone.utc)
        return v
