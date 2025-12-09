from os import SEEK_SET
from pathlib import Path
from typing import BinaryIO, Final, Self

from pydb import interface


class MonolithicFile(interface.File):
    """A monolithic file storage implementation where all data is stored in a single file per tablespace."""

    def __init__(self, tablespace: str, directory: Path | str, mode: interface.OpenFileMode = "rb"):
        """Initialize a monolithic file storage.

        Args:
            tablespace (str): The name of the tablespace (used as filename).
            directory (Path | str): The directory where the file will be stored.
            mode (interface.OpenFileMode, optional): The file open mode. Defaults to "rb".
        """

        super().__init__(tablespace=tablespace, directory=directory, mode=mode)

        self._path: Final[Path] = self._directory / f"{self._tablespace}.dblog"
        self._file: BinaryIO | None = None

    @property
    def closed(self) -> bool:
        """Check if the file is closed.

        Returns:
            bool: True if the file is closed or not opened, False otherwise.
        """

        return self._file is None or self._file.closed

    def _get_handle_or_raise(self) -> BinaryIO:
        """Retrieves the active file handle or raises an error if unavailable."""

        if self._file is None or self._file.closed:
            raise RuntimeError(f"MonolithicFile '{self._path.name}' is not open.")

        return self._file

    def write(self, data: bytes) -> int:
        """Write bytes to the file.

        Args:
            data (bytes): The bytes to write to the file.

        Raises:
            RuntimeError: If the file is not open.

        Returns:
            int: The number of bytes written.
        """

        return self._get_handle_or_raise().write(data)

    def read(self, size: int = -1) -> bytes:
        """Read bytes from the file.

        Args:
            size (int, optional): Number of bytes to read. -1 reads until EOF. Defaults to -1.

        Raises:
            RuntimeError: If the file is not open.

        Returns:
            bytes: The bytes read from the file.
        """

        return self._get_handle_or_raise().read(size)

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        """Move the file pointer to a specific position.

        Args:
            offset (int): The offset position.
            whence (int, optional): Reference point for offset (SEEK_SET, SEEK_CUR, SEEK_END). Defaults to SEEK_SET.

        Raises:
            RuntimeError: If the file is not open.

        Returns:
            int: The new absolute position in the file.
        """

        return self._get_handle_or_raise().seek(offset, whence)

    def tell(self) -> int:
        """Get the current file pointer position.

        Raises:
            RuntimeError: If the file is not open.

        Returns:
            int: The current position in the file.
        """

        return self._get_handle_or_raise().tell()

    def close(self) -> None:
        """Close the file.

        Flushes any buffered data before closing. Safe to call multiple times.
        """

        if self._file is not None and not self._file.closed:
            try:
                self._file.flush()
            finally:
                self._file.close()

        self._file = None

    def __enter__(self) -> Self:
        """Enter the context manager (opens the file).

        Creates the file if it doesn't exist and the mode is read.

        Returns:
            Self: The MonolithicFile instance.
        """

        if self._file is not None and not self._file.closed:
            return self

        if "r" in self._mode and not self._path.exists():
            self._path.touch(exist_ok=True)

        self._file = open(self._path, self._mode)  # pylint: disable=W1514

        return self

    def __exit__(self, *_: object) -> None:
        """Exit the context manager (closes the file).

        Args:
            *_ (object): Exception information (type, value, traceback).
        """

        self.close()
