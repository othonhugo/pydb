class PyDBError(Exception):
    """Base exception for all PyDB errors."""


class FileError(PyDBError):
    """Exception raised for file-related errors in PyDB."""


class IndexingError(PyDBError):
    """Exception raised for index-related errors in PyDB."""


class StorageError(PyDBError):
    """Exception raised for storage-related errors in PyDB."""
