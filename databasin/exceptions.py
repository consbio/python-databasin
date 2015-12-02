from requests.exceptions import HTTPError


class LoginError(HTTPError):
    """Raised when Data Basin login fails (mostly likely due to bad username/password)."""

    pass


class LoginRequiredError(HTTPError):
    """Raised when the client must be logged in to make the request."""

    pass


class ForbiddenError(HTTPError):
    """Raised when the client is logged in, but is not allowed to perform the requested action."""

    pass


class DatasetImportError(Exception):
    """Raised in response to an import failure."""

    pass
