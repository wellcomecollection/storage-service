# -*- encoding: utf-8


class StorageServiceException(Exception):
    """Base class for all storage service exceptions."""
    pass


class IngestNotFound(StorageServiceException):
    """Raised if you try to get an ingest that doesn't exist."""
    pass


class BagNotFound(StorageServiceException):
    """Raised if you try to get a bag that doesn't exist."""
    pass


class ServerError(StorageServiceException):
    """Raised if we get a 5xx Server Error from the storage service."""
    pass


class UserError(StorageServiceException):
    """Raised if we get a 4xx User Error from the storage service."""
    pass
