class ScraperException(Exception):
    """Base exception for this application."""
    pass

class PoemParsingError(ScraperException):
    """Raised when the structure of a poem cannot be parsed from wikitext."""
    pass

class PageProcessingError(ScraperException):
    """Raised when an entire page cannot be processed due to a non-recoverable error."""
    pass
