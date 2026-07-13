class CollectorError(Exception):
    """Base class for expected collector errors."""


class ResourceNotFoundError(CollectorError):
    """A requested service or device does not exist."""


class UpstreamUnavailableError(CollectorError):
    """A production data source is currently unavailable."""


class InvalidUpstreamResponseError(CollectorError):
    """An upstream service returned an invalid or incomplete response."""
