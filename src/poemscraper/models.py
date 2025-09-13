import datetime
from typing import List, Optional, Literal, Union
from pydantic import BaseModel, Field, HttpUrl

class BaseSchema(BaseModel):
    """Classe de base pour tous nos modèles avec des configurations communes."""
    class Config:
        orm_mode = True

class Author(BaseSchema):
    id: int = Field(..., description="ID unique de la catégorie auteur.")
    name: str = Field(..., description="Nom de l'auteur.")
    wikisource_url: HttpUrl = Field(..., description="URL de la page catégorie de l'auteur.")

class PoeticCollection(BaseSchema):
    id: int = Field(..., description="ID de la page du recueil.")
    title: str = Field(..., description="Titre du recueil.")
    author_id: int = Field(..., description="Clé étrangère vers l'auteur.")
    wikisource_url: HttpUrl = Field(..., description="URL de la page du recueil.")

class VersionHub(BaseSchema):
    """Représente une page qui est un portail vers plusieurs versions d'un même poème."""
    id: int = Field(..., description="ID de la page du portail de versions.")
    title: str = Field(..., description="Titre du poème (sans la version).")
    author_id: int = Field(..., description="Clé étrangère vers l'auteur.")

    collection_id: Optional[int] = Field(None, description="Clé étrangère optionnelle vers le recueil.")
    wikisource_url: HttpUrl = Field(..., description="URL de la page du portail.")


class PoemStructure(BaseSchema):
    """Structure normalisée du poème (strophes et vers)."""
    stanzas: List[List[str]] = Field(..., description="Liste de strophes, contenant des listes de vers.")
    raw_markers: List[str] = Field(default_factory=list, description="Marqueurs HTML bruts détectés pour les blocs de poèmes.")

class PoemMetadata(BaseSchema):
    """Conteneur structuré pour toutes les métadonnées extraites."""
    author_name: Optional[str] = Field(None, description="Auteur(s) principal(aux) du poème (extrait de la page).")
    publication_date: Optional[str] = Field(None, description="Date de publication (souvent l'année).")
    source_collection_name: Optional[str] = Field(None, description="Nom du recueil ou de la publication d'origine (extrait de la page).")
    publisher: Optional[str] = Field(None, description="Maison d'édition.")
    translator: Optional[str] = Field(None, description="Traducteur, si applicable.")
    license_name: Optional[str] = Field(None, description="Nom de la licence détectée.")

class Poem(BaseSchema):
    """Schéma JSON complet et validé pour un poème unique."""
    page_id: int = Field(..., description="Identifiant unique de la page MediaWiki (pageid).")
    revision_id: int = Field(..., description="Identifiant unique de la révision spécifique extraite (revid).")
    title: str = Field(..., description="Titre canonique de la page (poème).")
    language: str = Field(..., description="Code langue du projet Wikisource (ex: 'fr').")
    wikisource_url: HttpUrl = Field(..., description="URL canonique complète vers la page du poème.")

    author_id: int = Field(..., description="Clé étrangère vers l'auteur.")
    collection_id: Optional[int] = Field(None, description="Clé étrangère vers le recueil parent.")
    hub_id: Optional[int] = Field(None, description="Clé étrangère vers le portail de version parent.")

    metadata: PoemMetadata = Field(..., description="Toutes les métadonnées extraites.")
    structure: PoemStructure = Field(..., description="Structure parsée du poème.")
    normalized_text: str = Field(..., description="Texte complet du poème, nettoyé et concaténé.")
    raw_wikitext: str = Field(..., description="Le contenu wikitext complet et brut de la révision.")
    checksum_sha256: str = Field(..., description="SHA-256 du champ 'raw_wikitext' pour déduplication et intégrité.")
    extraction_timestamp: datetime.datetime = Field(..., description="Timestamp ISO 8601 (UTC) de l'extraction.")

ScrapedData = Union[Author, PoeticCollection, VersionHub, Poem]