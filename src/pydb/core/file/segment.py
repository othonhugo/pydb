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
        return self._path

    @property
    def size(self) -> int:
        try:
            return self._path.stat().st_size
        except FileNotFoundError:
            return 0

    @classmethod
    def from_filepath(cls, filepath: Path, *, root_directory: Path) -> Self:
        if not filepath.parent.samefile(root_directory):
            raise ValueError(f"File {filepath} is not inside {root_directory}")

        match = SEGMENT_PATTERN.match(filepath.name)

        if not match:
            raise ValueError(f"Invalid segment filename: {filepath.name}")

        return cls(index=int(match.group("index")), tablespace=match.group("tablespace"), directory=root_directory)

    def __lt__(self, other: Self) -> bool:
        return self.index < other.index


class SegmentedFile(interface.File):
    """
    Manages a collection of Segment files, providing a continuous, file-like interface for a segmented log system.

    It handles automatic rollover when segments reach max_size and seamless  seeking/reading across segment boundaries.
    """

    def __init__(self, tablespace: str, directory: Path | str, max_size: int, mode: interface.OpenFileMode = "rb"):
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
        return self._file is None or self._file.closed

    def _get_handle_or_raise(self) -> BinaryIO:
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
        """Closes current file (if any) and opens the segment at the specified index (updates base offsets logic)."""

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
        """Creates a new physical segment file, switches to it, and returns the handle."""

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
        handle = self._get_handle_or_raise()
        return self._current_segment_base_offset + handle.tell()

    def close(self) -> None:
        if self._file and not self._file.closed:
            self._file.close()

        self._file = None

    def __enter__(self) -> Self:
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
        self.close()
