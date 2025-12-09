from pydb import config, interface


class InMemoryIndexError(config.IndexingError):
    """Base exception for in memory index errors."""


class InMemoryIndexKeyNotFoundError(InMemoryIndexError):
    """Raised when a key is not found in the index table."""

    def __init__(self, *, key: bytes):
        self.key = key

        super().__init__(f"Key not found: {key!r}")


class InMemoryIndex(interface.Index):
    """In-memory implementation of the Index interface using a dictionary."""

    def __init__(self) -> None:
        """Initialize an empty in-memory index."""
        self._offset_table = dict[bytes, int]()

    def has(self, key: bytes, /) -> bool:
        """Check if a key exists in the index.

        Args:
            key (bytes): The key to check.

        Returns:
            bool: True if the key exists, False otherwise.
        """

        return key in self._offset_table

    def set(self, key: bytes, offset: int, /) -> None:
        """Set or update the offset for a key in the index.

        Args:
            key (bytes): The key to set.
            offset (int): The file offset where the key's data is stored.
        """

        self._offset_table[key] = offset

    def get(self, key: bytes, /) -> int:
        """Get the offset for a key from the index.

        Args:
            key (bytes): The key to retrieve.

        Raises:
            InMemoryIndexKeyNotFoundError: If the key is not found in the index.

        Returns:
            int: The file offset where the key's data is stored.
        """

        offset = self._offset_table.get(key)

        if offset is None:
            raise InMemoryIndexKeyNotFoundError(key=key)

        return offset

    def delete(self, key: bytes, /) -> None:
        """Delete a key from the index.

        This operation is idempotent - deleting a non-existent key has no effect.

        Args:
            key (bytes): The key to delete.
        """

        self._offset_table.pop(key, None)
