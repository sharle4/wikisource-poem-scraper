import datetime
from typing import List, Optional, Literal
from pydantic import BaseModel, Field, HttpUrl, field_validator


class PoemStructure(BaseModel):
    """Structure normalisée du poème (strophes et vers)."""

    stanzas: List[List[str]] = Field(
        ..., description="Liste de strophes, contenant des listes de vers."
    )
    raw_markers: List[str] = Field(
        default_factory=list,
        description="Marqueurs HTML bruts détectés pour les blocs de poèmes.",
    )


class PoemMetadata(BaseModel):
    """Conteneur structuré pour toutes les métadonnées extraites."""

    author: Optional[str] = Field(
        None, description="Auteur(s) principal(aux) du poème."
    )
    publication_date: Optional[str] = Field(
        None, description="Date de publication (souvent l'année)."
    )
    source_collection: Optional[str] = Field(
        None, description="Nom du recueil ou de la publication d'origine."
    )
    publisher: Optional[str] = Field(None, description="Maison d'édition.")
    translator: Optional[str] = Field(None, description="Traducteur, si applicable.")
    license_name: Optional[str] = Field(None, description="Nom de la licence détectée.")


class PoemSchema(BaseModel):
    """
    Schéma JSON complet et validé pour un poème unique.
    """

    page_id: int = Field(
        ..., description="Identifiant unique de la page MediaWiki (pageid)."
    )
    revision_id: int = Field(
        ..., description="Identifiant unique de la révision spécifique extraite (revid)."
    )
    title: str = Field(..., description="Titre canonique de la page (poème).")
    language: str = Field(
        ..., description="Code langue du projet Wikisource (ex: 'fr')."
    )
    wikisource_url: HttpUrl = Field(
        ..., description="URL canonique complète vers la page du poème."
    )

    hub_title: str = Field(
        ..., description="Titre de la page 'hub' de versions multiples parente, ou le titre du poème lui-même si autonome."
    )
    hub_page_id: int = Field(
        ..., description="ID de la page 'hub' de versions multiples parente, ou le page_id du poème lui-même si autonome."
    )

    metadata: PoemMetadata = Field(..., description="Toutes les métadonnées extraites.")
    structure: PoemStructure = Field(..., description="Structure parsée du poème.")

    normalized_text: str = Field(
        ...,
        description="Texte complet du poème, nettoyé et concaténé.",
    )
    raw_wikitext: str = Field(
        ..., description="Le contenu wikitext complet et brut de la révision."
    )

    checksum_sha256: str = Field(
        ...,
        description="SHA-256 du champ 'raw_wikitext' pour déduplication et intégrité.",
    )
    extraction_timestamp: datetime.datetime = Field(
        ..., description="Timestamp ISO 8601 (UTC) de l'extraction."
    )
    provenance: Literal["api"] = Field(
        "api", description="Source de la donnée."
    )

    @field_validator("extraction_timestamp", mode="before")
    @classmethod
    def set_default_timestamp(cls, v):
        """Assure que le timestamp est généré s'il n'est pas fourni."""
        if v is None:
            return datetime.datetime.now(datetime.timezone.utc)
        return v