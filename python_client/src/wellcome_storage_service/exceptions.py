# -*- encoding: utf-8

class StorageServiceException(Exception):
    """Base class for all storage service exceptions."""
    pass


class IngestNotFound(StorageServiceException):
    """Raised if you try to get an ingest that doesn't exist."""
    pass
