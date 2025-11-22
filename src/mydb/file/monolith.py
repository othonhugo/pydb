from os import SEEK_SET
from pathlib import Path
from typing import BinaryIO, Final, Self

from mydb.interface import File, OpenFileMode


class MonolithicFile(File):
    """A monolithic file storage implementation where all data is stored in a single file per tablespace."""

    def __init__(self, tablespace: str, directory: Path | str, mode: OpenFileMode = "rb"):
        super().__init__(tablespace=tablespace, directory=directory, mode=mode)

        self._path: Final[Path] = self._directory / f"{self._tablespace}.dblog"
        self._file: BinaryIO | None = None

    @property
    def closed(self) -> bool:
        return self._file is None or self._file.closed

    def _get_handle_or_raise(self) -> BinaryIO:
        """Retrieves the active file handle or raises an error if unavailable."""

        if self._file is None or self._file.closed:
            raise RuntimeError(f"MonolithicFile '{self._path.name}' is not open.")
        return self._file

    def write(self, data: bytes) -> int:
        return self._get_handle_or_raise().write(data)

    def read(self, size: int = -1) -> bytes:
        return self._get_handle_or_raise().read(size)

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        return self._get_handle_or_raise().seek(offset, whence)

    def tell(self) -> int:
        return self._get_handle_or_raise().tell()

    def close(self) -> None:
        if self._file is not None and not self._file.closed:
            try:
                self._file.flush()
            finally:
                self._file.close()

        self._file = None

    def __enter__(self) -> Self:
        if self._file is not None and not self._file.closed:
            return self

        if "r" in self._mode and not self._path.exists():
            self._path.touch(exist_ok=True)

        self._file = open(self._path, self._mode)  # pylint: disable=W1514

        return self

    def __exit__(self, *_: object) -> None:
        self.close()
