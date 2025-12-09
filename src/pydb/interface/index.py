from abc import ABC, abstractmethod


class Index(ABC):
    """Abstract base class for index implementations."""

    @abstractmethod
    def has(self, key: bytes, /) -> bool:
        """Check if a key exists in the index.

        Args:
            key (bytes): The key to check.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        Returns:
            bool: True if the key exists, False otherwise.
        """

        raise NotImplementedError

    @abstractmethod
    def set(self, key: bytes, offset: int, /) -> None:
        """Set or update the offset for a key in the index.

        Args:
            key (bytes): The key to set.
            offset (int): The file offset where the key's data is stored.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """

        raise NotImplementedError

    @abstractmethod
    def get(self, key: bytes, /) -> int:
        """Get the offset for a key from the index.

        Args:
            key (bytes): The key to retrieve.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        Returns:
            int: The file offset where the key's data is stored.
        """

        raise NotImplementedError

    @abstractmethod
    def delete(self, key: bytes, /) -> None:
        """Delete a key from the index.

        Args:
            key (bytes): The key to delete.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """

        raise NotImplementedError
