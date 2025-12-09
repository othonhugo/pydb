from abc import ABC, abstractmethod
from os import SEEK_SET
from pathlib import Path
from typing import Final, Literal, Self

OpenFileMode = Literal["rb", "ab", "r+b", "a+b", "wb", "w+b"]


class File(ABC):
    """Abstract base class for file storage implementations."""

    def __init__(self, tablespace: str, directory: Path | str, mode: OpenFileMode = "rb"):
        super().__init__()

        tablespace = tablespace.strip()

        if not tablespace:
            raise ValueError("Tablespace cannot be empty.")

        if mode not in ("rb", "ab", "r+b", "a+b", "wb", "w+b"):
            raise ValueError(f"Invalid mode: {mode}.")

        directory = Path(directory).resolve()

        if not directory.exists():
            raise FileNotFoundError(f"Directory does not exist: {directory}")

        if not directory.is_dir():
            raise NotADirectoryError(f"Path exists but is not a directory: {directory}")

        self._tablespace: Final[str] = tablespace
        self._directory: Final[Path] = directory
        self._mode: Final[OpenFileMode] = mode

    @abstractmethod
    def write(self, data: bytes) -> int:
        """Write bytes to the file.

        Args:
            data (bytes): The bytes to write to the file.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        Returns:
            int: The number of bytes written.
        """

        raise NotImplementedError

    @abstractmethod
    def read(self, size: int = -1) -> bytes:
        """Read bytes from the file.

        Args:
            size (int, optional): Number of bytes to read. -1 reads until EOF. Defaults to -1.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        Returns:
            bytes: The bytes read from the file.
        """

        raise NotImplementedError

    @abstractmethod
    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        """Move the file pointer to a specific position.

        Args:
            offset (int): The offset position.
            whence (int, optional): Reference point for offset (SEEK_SET, SEEK_CUR, SEEK_END). Defaults to SEEK_SET.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        Returns:
            int: The new absolute position in the file.
        """

        raise NotImplementedError

    @abstractmethod
    def tell(self) -> int:
        """Get the current file pointer position.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        Returns:
            int: The current position in the file.
        """

        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """Close the file.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """

        raise NotImplementedError

    @property
    @abstractmethod
    def closed(self) -> bool:
        """Check if the file is closed.

        Raises:
            NotImplementedError: This property must be implemented by subclasses.

        Returns:
            bool: True if the file is closed, False otherwise.
        """

        raise NotImplementedError

    @abstractmethod
    def __enter__(self) -> Self:
        """Enter the context manager (opens the file).

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        Returns:
            Self: The file instance.
        """

        raise NotImplementedError

    @abstractmethod
    def __exit__(self, *_: object) -> None:
        """Exit the context manager (closes the file).

        Args:
            *_ (object): Exception information (type, value, traceback).

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """

        raise NotImplementedError
