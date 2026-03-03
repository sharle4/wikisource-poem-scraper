import pytest
from src.scriptorium.author_cleaner import clean_author_name

def test_clean_author_name_prefixes():
    assert clean_author_name("Auteur : André Theuriet") == "André Theuriet"
    assert clean_author_name("Auteur : François Coppée") == "François Coppée"
    assert clean_author_name("Auteur:Philippe Desportes") == "Philippe Desportes"

def test_clean_author_name_suffixes():
    assert clean_author_name("(Alphonse de Lamartine) -non signé") == "Alphonse de Lamartine"
    assert clean_author_name("Victor Hugo - non signé") == "Victor Hugo"
    
def test_clean_author_name_author_mention():
    assert clean_author_name("Théodore Hannon, auteur du texte") == "Théodore Hannon"
    
def test_clean_author_name_multiple_authors_and_punctuation():
    assert clean_author_name("Théodore Hannon, auteur du texte ;Joris Karl Huysmans") == "Théodore Hannon ; Joris Karl Huysmans"
    
def test_clean_author_name_translator():
    assert clean_author_name("Henrik IbsenTraduit parA. Matthey") == "Henrik Ibsen"
    assert clean_author_name("William Shakespeare Traduit par François-Victor Hugo") == "William Shakespeare"

def test_clean_author_name_normalization():
    assert clean_author_name("A. de Lamartine") == "Alphonse de Lamartine"
    assert clean_author_name("Tarass Chevtchenko") == "Taras Chevtchenko"
    assert clean_author_name("Oscar Vladislas de Lubicz Milosz") == "Oscar Venceslas de Lubicz-Milosz"
    assert clean_author_name("Nikolaus Becker,Alphonse de Lamartine") == "Nikolaus Becker, Alphonse de Lamartine"

def test_clean_author_name_parentheses():
    assert clean_author_name("(Alphonse de Lamartine)") == "Alphonse de Lamartine"
