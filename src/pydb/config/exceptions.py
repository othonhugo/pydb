class PyDBError(Exception):
    pass


class FileError(PyDBError):
    pass


class IndexError(PyDBError):
    pass


class StorageError(PyDBError):
    pass
