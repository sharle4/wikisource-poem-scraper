import re

AUTHOR_NORMALIZATION = {
    # Lamartine
    "A. de Lamartine": "Alphonse de Lamartine",
    "Lamartine": "Alphonse de Lamartine",
    "Nikolaus Becker,Alphonse de Lamartine": "Nikolaus Becker, Alphonse de Lamartine",
    # Chevtchenko
    "Tarass Chevtchenko": "Taras Chevtchenko",
    "Taras grigoriévitch chevtchenko": "Taras Chevtchenko",
    # Milosz
    "Oscar Vladislas de Lubicz Milosz": "Oscar Venceslas de Lubicz-Milosz",
    "Oscar Venceslas de Lubicz-Milosz": "Oscar Venceslas de Lubicz-Milosz",
}

def clean_author_name(raw_name: str) -> str:
    """
    Cleans and normalizes an author name extracted from Wikisource.
    """
    if not isinstance(raw_name, str):
        return raw_name

    name = raw_name.strip()
    if not name:
        return name

    # Remove prefixes
    name = re.sub(r"^[Aa]uteur\s*:\s*", "", name)

    # Remove irrelevant mentions
    name = re.sub(r"\s*-\s*non signé", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\s*,?\s*auteur du texte", "", name, flags=re.IGNORECASE)
    name = re.sub(r"Traduit par.*$", "", name, flags=re.IGNORECASE)

    # Fix missing spaces around punctuation: ";Joris" -> "; Joris"
    name = re.sub(r";([A-Z])", r"; \1", name)
    name = re.sub(r",([A-Z])", r", \1", name)

    # Unwrap parentheses if it exactly matches (Text)
    m = re.match(r"^\((.*?)\)$", name)
    if m:
        name = m.group(1).strip()

    # Extra whitespace
    name = re.sub(r"\s+", " ", name).strip()

    # Apply normalizations
    for key, canonical in AUTHOR_NORMALIZATION.items():
        if name.lower() == key.lower():
            return canonical

    return name
