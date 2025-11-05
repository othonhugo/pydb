from abc import ABC, abstractmethod
from os import SEEK_SET
from typing import Literal, Self

OpenFileMode = Literal["rb", "ab", "r+b", "a+b", "wb", "w+b"]


class File(ABC):
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
