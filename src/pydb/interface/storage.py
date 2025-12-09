from abc import ABC, abstractmethod


class StorageEngine(ABC):
    """Abstract base class for storage engine implementations."""

    @abstractmethod
    def set(self, key: bytes, value: bytes, /) -> None:
        """Store a key-value pair in the storage engine.

        Args:
            key (bytes): The key to store.
            value (bytes): The value to store.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """

        raise NotImplementedError

    @abstractmethod
    def get(self, key: bytes, /) -> bytes:
        """Retrieve the value for a key from the storage engine.

        Args:
            key (bytes): The key to retrieve.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        Returns:
            bytes: The value associated with the key.
        """

        raise NotImplementedError

    @abstractmethod
    def delete(self, key: bytes, /) -> None:
        """Delete a key-value pair from the storage engine.

        Args:
            key (bytes): The key to delete.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """

        raise NotImplementedError
