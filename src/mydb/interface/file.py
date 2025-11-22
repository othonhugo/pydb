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
        raise NotImplementedError

    @abstractmethod
    def read(self, size: int = -1) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        raise NotImplementedError

    @abstractmethod
    def tell(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def closed(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def __enter__(self) -> Self:
        raise NotImplementedError

    @abstractmethod
    def __exit__(self, *_: object) -> None:
        raise NotImplementedError
