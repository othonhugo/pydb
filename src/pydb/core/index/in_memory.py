from pydb import config, interface


class InMemoryIndexError(config.IndexError):
    """Base exception for in memory index errors."""


class InMemoryIndexKeyNotFoundError(InMemoryIndexError):
    """Raised when a key is not found in the index table."""

    def __init__(self, *, key: bytes):
        self.key = key

        super().__init__(f"Key not found: {key!r}")


class InMemoryIndex(interface.Index):
    def __init__(self) -> None:
        self._offset_table = dict[bytes, int]()

    def has(self, key: bytes, /) -> bool:
        return key in self._offset_table

    def set(self, key: bytes, offset: int, /) -> None:
        self._offset_table[key] = offset

    def get(self, key: bytes, /) -> int:
        offset = self._offset_table.get(key)

        if offset is None:
            raise InMemoryIndexKeyNotFoundError(key=key)

        return offset

    def delete(self, key: bytes, /) -> None:
        self._offset_table.pop(key, None)
