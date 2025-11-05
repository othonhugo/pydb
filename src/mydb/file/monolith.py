from os import SEEK_SET
from pathlib import Path
from typing import BinaryIO, Self

from mydb.interface import File, OpenFileMode


class MonolithicStorage(File):
    # pylint: disable=W1514

    def __init__(self, tablespace: str, directory: Path | str, mode: OpenFileMode = "rb"):
        # pylint: disable=R0801

        tablespace = tablespace.strip()

        if not tablespace:
            raise ValueError("Tablespace cannot be empty or whitespace only")

        directory = Path(directory).resolve()

        if not directory.exists():
            raise FileNotFoundError(f"Directory does not exist: {directory}")

        if not directory.is_dir():
            raise NotADirectoryError(f"Path exists but is not a directory: {directory}")

        self._tablespace = tablespace
        self._directory = directory
        self._mode: OpenFileMode = mode

        self.path.touch(exist_ok=True)

        self._file: BinaryIO | None = None

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
            self._file = open(self.path, self._mode)

        return self

    def __exit__(self, *_: object) -> None:
        self.close()
