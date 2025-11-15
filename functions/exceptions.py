class OpenPechaException(Exception):
    status_code = 500

    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def to_dict(self):
        return {"error": self.message}


class DataNotFound(OpenPechaException):
    status_code = 404


class InvalidRequest(OpenPechaException):
    status_code = 400


class DataConflict(OpenPechaException):
    status_code = 409


class ValidationError(OpenPechaException):
    status_code = 422
