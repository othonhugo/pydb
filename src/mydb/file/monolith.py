from os import SEEK_SET
from pathlib import Path
from typing import BinaryIO, Self

from mydb.interface import File, OpenFileMode


class MonolithicFile(File):
    def __init__(self, tablespace: str, directory: Path | str, mode: OpenFileMode = "rb"):
        super().__init__(tablespace=tablespace, directory=directory)

        self._mode: OpenFileMode = mode
        self._file: BinaryIO | None = None

        self.path.touch(exist_ok=True)

    @property
    def path(self) -> Path:
        filename = f"{self._tablespace}.dblog"

        return self._directory / filename

    @property
    def size(self) -> int:
        try:
            return self.path.stat().st_size
        except FileNotFoundError:
            return 0

    def _ensure_file_open(self) -> BinaryIO:
        if self._file is None or self._file.closed:
            raise RuntimeError("MonolithicStorage is not opened.")

        return self._file

    @property
    def closed(self) -> bool:
        if not self._file:
            return True

        return self._file.closed

    def write(self, data: bytes) -> int:
        f = self._ensure_file_open()

        return f.write(data)

    def read(self, size: int = -1) -> bytes:
        f = self._ensure_file_open()

        return f.read(size)

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        f = self._ensure_file_open()

        return f.seek(offset, whence)

    def tell(self) -> int:
        f = self._ensure_file_open()

        return f.tell()

    def close(self) -> None:
        f = self._ensure_file_open()

        f.close()

    def __enter__(self) -> Self:
        if self._file is None or self._file.closed:
            self._file = open(self.path, self._mode)  # pylint: disable=W1514,R1732

        return self

    def __exit__(self, *_: object) -> None:
        self.close()
