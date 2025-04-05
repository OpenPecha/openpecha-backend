class OpenPechaException(Exception):
    """Base exception for all OpenPecha custom exceptions."""

    status_code = 500

    def __init__(self, message):
        super().__init__()
        self.message = message

    def to_dict(self):
        return {"error": self.message}


class DataNotFound(OpenPechaException):
    """Exception for data not found in the database."""

    status_code = 404


class InvalidRequest(OpenPechaException):
    """Exception for invalid API requests."""

    status_code = 400
