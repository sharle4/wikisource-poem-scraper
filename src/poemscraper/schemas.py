import datetime
from typing import List, Optional, Literal, Union
from pydantic import BaseModel, Field, HttpUrl, field_validator


class PoemInfo(BaseModel):
    """Représente un poème unique au sein d'un recueil."""
    title: str
    page_id: int
    url: HttpUrl


class Section(BaseModel):
    """Représente une section titrée dans un recueil."""
    title: str
    poems: List[PoemInfo] = Field(default_factory=list)


CollectionComponent = Union[Section, PoemInfo]


class Collection(BaseModel):
    """Modélise la structure complète d'un recueil de poèmes."""
    page_id: int
    title: str
    author: Optional[str] = None
    url: HttpUrl
    content: List[CollectionComponent] = Field(default_factory=list)


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


class PoemSchema(BaseModel):
    """
    Schéma JSON complet et validé pour un poème unique, maintenant enrichi
    avec les informations structurelles du recueil.
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

    collection_page_id: Optional[int] = Field(
        None, description="ID de la page du recueil parent."
    )
    collection_title: Optional[str] = Field(
        None, description="Titre du recueil parent."
    )
    section_title: Optional[str] = Field(
        None, description="Titre de la section du poème dans le recueil."
    )
    poem_order: Optional[int] = Field(
        None, description="Position ordinale du poème dans le recueil (commence à 0)."
    )
    hub_title: Optional[str] = Field(
        None, description="Titre de la page 'hub' de versions multiples parente."
    )
    hub_page_id: int = Field(
        ..., description="ID unique du groupe de poèmes. C'est le page_id du hub parent, ou le page_id du poème lui-même s'il est autonome."
    )

    metadata: PoemMetadata = Field(..., description="Toutes les métadonnées extraites.")
    structure: PoemStructure = Field(..., description="Structure parsée du poème.")
    collection_structure: Optional[Collection] = Field(
        None, description="Objet complet décrivant la structure du recueil parent (présent uniquement pour le premier poème d'un recueil pour éviter la duplication massive de données)."
    )
    normalized_text: str = Field(
        ..., description="Texte complet du poème, nettoyé et concaténé."
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