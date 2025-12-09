import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, Final, List, Self

from pydb import interface

SEGMENT_PATTERN = re.compile(r"^(?P<tablespace>[a-zA-Z0-9_-]+)_(?P<index>\d{10})\.dblog$")


@dataclass(frozen=True)
class Segment:
    """Represents a single segment file within a segmented log system."""

    index: int
    tablespace: str
    directory: Path

    _path: Path = field(init=False)

    def __post_init__(self) -> None:
        filename = f"{self.tablespace}_{self.index:010d}.dblog"

        object.__setattr__(self, "_path", self.directory / filename)

    @property
    def path(self) -> Path:
        """Get the full file path for this segment.

        Returns:
            Path: The absolute path to the segment file.
        """

        return self._path

    @property
    def size(self) -> int:
        """Get the current size of the segment file.

        Returns:
            int: The size in bytes, or 0 if the file doesn't exist.
        """

        try:
            return self._path.stat().st_size
        except FileNotFoundError:
            return 0

    @classmethod
    def from_filepath(cls, filepath: Path, *, root_directory: Path) -> Self:
        """Create a Segment instance from a file path.

        Args:
            filepath (Path): The path to the segment file.
            root_directory (Path): The root directory containing the segment.

        Raises:
            ValueError: If the file is not in the root directory or has an invalid name.

        Returns:
            Self: A new Segment instance.
        """

        if not filepath.parent.samefile(root_directory):
            raise ValueError(f"File {filepath} is not inside {root_directory}")

        match = SEGMENT_PATTERN.match(filepath.name)

        if not match:
            raise ValueError(f"Invalid segment filename: {filepath.name}")

        return cls(index=int(match.group("index")), tablespace=match.group("tablespace"), directory=root_directory)

    def __lt__(self, other: Self) -> bool:
        """Compare segments by index for sorting.

        Args:
            other (Self): Another Segment instance to compare with.

        Returns:
            bool: True if this segment's index is less than the other's.
        """
        return self.index < other.index


class SegmentedFile(interface.File):
    """
    Manages a collection of Segment files, providing a continuous, file-like interface for a segmented log system.

    It handles automatic rollover when segments reach max_size and seamless  seeking/reading across segment boundaries.
    """

    def __init__(self, tablespace: str, directory: Path | str, max_size: int, mode: interface.OpenFileMode = "rb"):
        """Initialize a segmented file storage.

        Args:
            tablespace (str): The name of the tablespace.
            directory (Path | str): The directory where segment files will be stored.
            max_size (int): Maximum size in bytes for each segment file.
            mode (interface.OpenFileMode, optional): The file open mode. Defaults to "rb".

        Raises:
            ValueError: If max_size is less than or equal to 0.
        """

        super().__init__(tablespace=tablespace, directory=directory, mode=mode)

        if max_size <= 0:
            raise ValueError("max_size must be > 0")

        self._max_size: Final[int] = max_size

        self._segments: List[Segment] = []
        self._file: BinaryIO | None = None

        self._current_segment_index: int = -1
        self._current_segment_base_offset: int = 0

    @property
    def closed(self) -> bool:
        """Check if the file is closed.

        Returns:
            bool: True if the file is closed or not opened, False otherwise.
        """

        return self._file is None or self._file.closed

    def _get_handle_or_raise(self) -> BinaryIO:
        """Get the active file handle or raise an error.

        Raises:
            RuntimeError: If the SegmentedFile is not open.

        Returns:
            BinaryIO: The active file handle.
        """

        if self._file is None or self._file.closed:
            raise RuntimeError(f"SegmentedFile '{self._tablespace}' is not open.")

        return self._file

    def _load_segments(self) -> None:
        """Scans directory for existing segments and populates the internal list."""

        self._segments.clear()

        glob_pattern = f"{self._tablespace}_*.dblog"

        for filepath in self._directory.glob(glob_pattern):
            try:
                seg = Segment.from_filepath(filepath, root_directory=self._directory)
                self._segments.append(seg)
            except ValueError:
                continue

        self._segments.sort(key=lambda s: s.index)

    def _activate_segment(self, index: int) -> BinaryIO:
        """Activate a specific segment by index.

        Closes the current file (if any) and opens the segment at the specified index.
        Updates the base offset logic for global positioning.

        Args:
            index (int): The index of the segment to activate.

        Raises:
            IndexError: If the segment index is out of bounds.

        Returns:
            BinaryIO: The file handle for the activated segment.
        """

        if not 0 <= index < len(self._segments):
            raise IndexError(f"Segment index {index} out of bounds.")

        if self._file and not self._file.closed:
            self._file.close()

        self._current_segment_index = index
        segment = self._segments[index]

        self._file = open(segment.path, self._mode)  # pylint: disable=W1514,R1732
        self._current_segment_base_offset = sum(s.size for s in self._segments[:index])

        return self._file

    def _create_and_activate_next_segment(self) -> BinaryIO:
        """Create and activate the next segment file.

        Creates a new physical segment file, switches to it, and returns the handle.

        Returns:
            BinaryIO: The file handle for the new segment.
        """

        next_index = 0

        if self._segments:
            next_index = self._segments[-1].index + 1

        new_seg = Segment(index=next_index, tablespace=self._tablespace, directory=self._directory)
        new_seg.path.touch()

        self._segments.append(new_seg)

        return self._activate_segment(len(self._segments) - 1)

    def _delete_all_segments(self) -> None:
        """Wipes physical files. Used in 'w' mode."""

        if not self._segments:
            self._load_segments()

        for seg in self._segments:
            seg.path.unlink(missing_ok=True)

        self._segments.clear()

        self._current_segment_index = -1
        self._current_segment_base_offset = 0

    def write(self, data: bytes) -> int:
        """Write bytes to the segmented file.

        Automatically creates new segments when the current segment reaches max_size.
        Handles writing across segment boundaries.

        Args:
            data (bytes): The bytes to write to the file.

        Raises:
            IOError: If the file is not open for writing.
            RuntimeError: If the file is not open.

        Returns:
            int: The total number of bytes written.
        """

        if "r" in self._mode and "+" not in self._mode:
            raise IOError("File not open for writing")

        handle = self._get_handle_or_raise()

        total_written = 0
        view = memoryview(data)

        while total_written < len(data):
            current_pos = handle.tell()
            space_left = self._max_size - current_pos

            if space_left <= 0:
                handle = self._create_and_activate_next_segment()

                current_pos = handle.tell()
                space_left = self._max_size - current_pos

            chunk_size = min(len(data) - total_written, space_left)

            bytes_written = handle.write(view[total_written : total_written + chunk_size])
            total_written += bytes_written

        return total_written

    def read(self, size: int = -1) -> bytes:
        """Read bytes from the segmented file.

        Seamlessly reads across segment boundaries if necessary.

        Args:
            size (int, optional): Number of bytes to read. -1 reads until EOF. Defaults to -1.

        Raises:
            IOError: If the file is not open for reading.
            RuntimeError: If the file is not open.

        Returns:
            bytes: The bytes read from the file.
        """

        if "w" in self._mode and "+" not in self._mode:
            raise IOError("File not open for reading")

        handle = self._get_handle_or_raise()
        chunks: List[bytes] = []
        bytes_read = 0

        while size == -1 or bytes_read < size:
            request_size = -1 if size == -1 else (size - bytes_read)
            chunk = handle.read(request_size)

            if chunk:
                chunks.append(chunk)
                bytes_read += len(chunk)

            if not chunk or (size != -1 and len(chunk) < request_size):
                if self._current_segment_index + 1 < len(self._segments):
                    handle = self._activate_segment(self._current_segment_index + 1)
                else:
                    break

        return b"".join(chunks)

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        """Move the file pointer to a specific position.

        Handles seeking across segment boundaries by calculating global offsets
        and activating the appropriate segment.

        Args:
            offset (int): The offset position.
            whence (int, optional): Reference point for offset (SEEK_SET, SEEK_CUR, SEEK_END). Defaults to SEEK_SET.

        Raises:
            ValueError: If whence is invalid.
            RuntimeError: If the file is not open.

        Returns:
            int: The new absolute position in the file.
        """

        handle = self._get_handle_or_raise()

        total_size = sum(s.size for s in self._segments)
        target_global_offset = 0

        if whence == os.SEEK_SET:
            target_global_offset = offset
        elif whence == os.SEEK_CUR:
            target_global_offset = self.tell() + offset
        elif whence == os.SEEK_END:
            target_global_offset = total_size + offset
        else:
            raise ValueError(f"Invalid whence: {whence}")

        target_global_offset = max(0, target_global_offset)

        current_seg = self._segments[self._current_segment_index]
        curr_start = self._current_segment_base_offset
        curr_end = curr_start + current_seg.size

        if curr_start <= target_global_offset <= curr_end:
            handle.seek(target_global_offset - curr_start)

            return target_global_offset

        accumulated = 0
        found = False

        for i, seg in enumerate(self._segments):
            seg_size = seg.size
            if accumulated <= target_global_offset < (accumulated + seg_size):
                handle = self._activate_segment(i)
                handle.seek(target_global_offset - accumulated)
                found = True

                break

            accumulated += seg_size

        if not found:
            if self._segments:
                handle = self._activate_segment(len(self._segments) - 1)
                local_offset = target_global_offset - self._current_segment_base_offset

                handle.seek(local_offset)
            else:
                if "w" in self._mode or "a" in self._mode:
                    self._create_and_activate_next_segment()
                else:
                    return 0

        return target_global_offset

    def tell(self) -> int:
        """Get the current file pointer position.

        Returns the global position across all segments.

        Raises:
            RuntimeError: If the file is not open.

        Returns:
            int: The current global position in the file.
        """

        handle = self._get_handle_or_raise()

        return self._current_segment_base_offset + handle.tell()

    def close(self) -> None:
        """Close the currently active segment file.

        Safe to call multiple times.
        """
        if self._file and not self._file.closed:
            self._file.close()

        self._file = None

    def __enter__(self) -> Self:
        """Enter the context manager (opens the file).

        Creates the directory if it doesn't exist. In write mode, deletes all existing
        segments. Loads existing segments or creates a new one as needed.

        Raises:
            FileNotFoundError: If no segments exist and the mode doesn't allow creation.

        Returns:
            Self: The SegmentedFile instance.
        """

        if self._file is not None:
            return self

        self._directory.mkdir(parents=True, exist_ok=True)

        if "w" in self._mode:
            self._delete_all_segments()
        else:
            self._load_segments()

        can_create = "w" in self._mode or "a" in self._mode

        if not self._segments:
            if not can_create:
                raise FileNotFoundError(f"No segments found in {self._directory}")

            self._create_and_activate_next_segment()
        else:
            if "a" in self._mode:
                handle = self._activate_segment(len(self._segments) - 1)
                handle.seek(0, os.SEEK_END)
            else:
                handle = self._activate_segment(0)
                handle.seek(0, os.SEEK_SET)

        return self

    def __exit__(self, *_: object) -> None:
        """Exit the context manager (closes the file).

        Args:
            *_ (object): Exception information (type, value, traceback).
        """

        self.close()
