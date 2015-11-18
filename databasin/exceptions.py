from requests.exceptions import HTTPError


class LoginError(HTTPError):
    """Raised when Data Basin login fails (mostly likely due to bad username/password)."""

    pass
