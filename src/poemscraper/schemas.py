import datetime
from typing import List, Dict, Any, Optional, Tuple, Literal
from pydantic import BaseModel, Field, HttpUrl, field_validator

class PoemLicense(BaseModel):
    """Schéma de la licence du document."""
    name: str = Field(..., description="Nom de la licence (ex: Creative Commons BY-SA 3.0)")
    url: Optional[HttpUrl] = Field(None, description="Lien direct vers le texte de la licence")

class PoemStructure(BaseModel):
    """Structure normalisée du poème (strophes et vers)."""
    stanzas: List[List[str]] = Field(..., description="Liste de strophes, contenant des listes de vers.")
    raw_markers: List[str] = Field(default_factory=list, description="Marqueurs Wikitext bruts détectés pour les strophes.")

class PoemSchema(BaseModel):
    """
    Schéma JSON complet pour un poème unique extrait de Wikisource.
    Conforme au format NDJSON attendu.
    """
    page_id: int = Field(..., description="Identifiant unique de la page MediaWiki (pageid).")
    revision_id: int = Field(..., description="Identifiant unique de la révision spécifique extraite (revid).")
    title: str = Field(..., description="Titre canonique de la page (poème).")
    author: Optional[str] = Field(None, description="Auteur(s) principal(aux) extrait(s) (souvent via templates).")
    author_url: Optional[HttpUrl] = Field(None, description="Lien vers la page auteur sur Wikisource (si trouvée).")
    language: str = Field(..., description="Code langue du projet Wikisource (ex: 'fr', 'en').")
    wikisource_url: HttpUrl = Field(..., description="URL canonique complète vers la page du poème.")
    
    license: Optional[PoemLicense] = Field(None, description="Information de licence détectée.")

    metadata: Dict[str, Any] = Field(default_factory=dict, description="Conteneur pour métadonnées diverses.")

    raw_wikitext: str = Field(..., description="Le contenu wikitext complet et brut de la révision.")
    rendered_html: Optional[str] = Field(None, description="Contenu HTML rendu par l'API (action=parse), si demandé.")
    
    structure: PoemStructure = Field(..., description="Structure parsée du poème (strophes et vers).")
    
    normalized_text: str = Field(..., description="Texte complet du poème, nettoyé et concaténé (vers séparés par \\n, strophes par \\n\\n).")
    
    checksum_sha256: str = Field(..., description="SHA-256 du champ 'raw_wikitext' pour déduplication et intégrité.")
    extraction_timestamp: datetime.datetime = Field(..., description="Timestamp ISO 8601 (UTC) de l'extraction.")
    provenance: Literal["api", "dump", "html"] = Field("api", description="Source de la donnée (actuellement 'api' par défaut).")

    @field_validator('extraction_timestamp', mode='before')
    @classmethod
    def set_default_timestamp(cls, v):
        """Assure que le timestamp est généré s'il n'est pas fourni."""
        if v is None:
            return datetime.datetime.now(datetime.timezone.utc)
        return v