from os import SEEK_CUR, SEEK_END, SEEK_SET
from pathlib import Path
from re import compile as compile_re
from typing import BinaryIO, List, Optional, Self

from mydb.interface import File, OpenFileMode


class Segment:
    """Represents a single segment file within a segmented log system."""

    FILENAME_PATTERN = compile_re(r"^(?P<tablespace>[a-zA-Z0-9_-]+)_(?P<index>\d{10})\.dblog$")

    def __init__(self, index: int, tablespace: str, directory: Path | str):
        if index < 0:
            raise ValueError("Segment index must be non-negative")

        tablespace = tablespace.strip()

        if not tablespace:
            raise ValueError("Tablespace cannot be empty or whitespace only")

        directory = Path(directory).resolve()

        if not directory.exists():
            raise FileNotFoundError(f"Directory does not exist: {directory}")

        if not directory.is_dir():
            raise NotADirectoryError(f"Path exists but is not a directory: {directory}")

        self.index = index
        self.tablespace = tablespace
        self.directory = directory

    @property
    def path(self) -> Path:
        filename = f"{self.tablespace}_{self.index:010d}.dblog"

        return self.directory / filename

    @property
    def size(self) -> int:
        try:
            return self.path.stat().st_size
        except FileNotFoundError:
            return 0

    @classmethod
    def from_filepath(cls, filepath: Path, *, directory: Path) -> Self:
        filepath = filepath.resolve()
        directory = directory.resolve()

        if not filepath.exists():
            raise FileNotFoundError(f"Segment file not found: {filepath}")

        if not filepath.parent.samefile(directory):
            raise ValueError(f"File {filepath!r} is not within the directory {directory!r}.")

        match = cls.FILENAME_PATTERN.match(filepath.name)

        if not match:
            raise ValueError(f"Invalid segment filename format: {filepath.name}")

        tablespace = match.group("tablespace")
        index = int(match.group("index"))

        return cls(index=index, tablespace=tablespace, directory=directory)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Segment):
            return NotImplemented

        same_index = self.index == other.index
        same_tablespace = self.tablespace == other.tablespace
        same_directory = self.directory.samefile(other.directory.resolve())

        return same_index and same_tablespace and same_directory

    def __lt__(self, other: Self) -> bool:
        return self.index < other.index


class SegmentedFile(File):
    # pylint: disable=W1514,R1732

    """Manages a collection of Segment files, providing a continuous, file-like interface for a segmented log system."""

    def __init__(self, tablespace: str, directory: Path | str, max_size: int, mode: OpenFileMode = "rb"):
        # pylint: disable=R0801

        if not tablespace:
            raise ValueError("Tablespace cannot be empty.")

        if max_size <= 0:
            raise ValueError("max_size must be a positive integer (greater than 0).")

        if mode not in ("rb", "ab", "r+b", "a+b", "wb", "w+b"):
            raise ValueError(f"Invalid mode: {mode}.")

        self._tablespace = tablespace
        self._directory = Path(directory)
        self._max_size = max_size

        self._mode: OpenFileMode = mode
        self._file_handle: Optional[BinaryIO] = None
        self._segments: List[Segment] = []

        self._current_segment_index = -1

        self._directory.mkdir(parents=True, exist_ok=True)
        self._load_segments()

        if "w" in self._mode:
            self._delete_all_segments()
            self._bump_new_segment()

        elif not self._segments:
            if "a" in self._mode:
                self._bump_new_segment()
            else:
                raise FileNotFoundError(f"No log segments found in '{self._directory}' for reading (mode: '{self._mode}').")

    def _load_segments(self) -> None:
        self._segments = []

        glob_pattern = f"{self._tablespace}_*.dblog"

        for filepath in self._directory.glob(glob_pattern):
            try:
                segment = Segment.from_filepath(filepath, directory=self._directory)
                self._segments.append(segment)
            except (ValueError, FileNotFoundError):
                pass

        self._segments.sort()

    def _create_new_segment(self, index: int) -> Segment:
        new_segment = Segment(index=index, tablespace=self._tablespace, directory=self._directory)
        new_segment.path.touch()

        self._segments.append(new_segment)
        self._segments.sort()

        return new_segment

    def _bump_new_segment(self) -> Segment:
        new_segment_index = (self._segments[-1].index + 1) if self._segments else 0
        new_segment = self._create_new_segment(new_segment_index)

        self._current_segment_index = self._segments.index(new_segment)

        return new_segment

    def _delete_all_segments(self) -> None:
        pattern = f"{self._tablespace}_*.dblog"

        for filepath in self._directory.glob(pattern):
            try:
                filepath.unlink(missing_ok=True)
            except OSError as e:
                raise OSError(f"Failed to delete segment file '{filepath.name}': {e}") from e

        self._segments.clear()
        self._current_segment_index = -1

    def _ensure_file_open(self) -> BinaryIO:
        if self._file_handle is None or self._file_handle.closed:
            raise RuntimeError("MonolithicStorage is not opened.")

        return self._file_handle

    def _rollover(self) -> None:
        if self._file_handle:
            self._file_handle.close()
            self._file_handle = None

        self._bump_new_segment()

        if not (active_segment := self.active_segment):
            raise RuntimeError("Failed to activate a new segment after rollover.")

        self._file_handle = open(active_segment.path, self._mode)

        if "a" in self._mode:
            self._file_handle.seek(0, SEEK_END)

    def _get_global_offset_from_current_position(self) -> int:
        if self._file_handle is None or self.active_segment is None:
            return 0

        global_offset = 0

        for i in range(self._current_segment_index):
            global_offset += self._segments[i].size

        global_offset += self._file_handle.tell()

        return global_offset

    def _set_position_from_global_offset(self, target_global_offset: int) -> int:
        if not self._segments:
            if self._file_handle:
                self.close()
            return 0

        target_global_offset = max(0, target_global_offset)

        current_accumulated_size = 0
        new_segment_index = 0
        internal_offset = 0

        for i, segment in enumerate(self._segments):
            segment_size = segment.size

            if target_global_offset < current_accumulated_size + segment_size:
                new_segment_index = i
                internal_offset = target_global_offset - current_accumulated_size

                break
            current_accumulated_size += segment_size
        else:
            new_segment_index = len(self._segments) - 1

            internal_offset = self._segments[new_segment_index].size
            target_global_offset = current_accumulated_size

        if self._current_segment_index != new_segment_index or self._file_handle is None or self._file_handle.closed:
            self.close()

            self._current_segment_index = new_segment_index
            active_segment = self._segments[self._current_segment_index]
            self._file_handle = open(active_segment.path, self._mode)

        self._file_handle.seek(internal_offset, SEEK_SET)

        return self._get_global_offset_from_current_position()

    @property
    def active_segment(self) -> Optional[Segment]:
        if 0 <= self._current_segment_index < len(self._segments):
            return self._segments[self._current_segment_index]

        return None

    def write(self, data: bytes) -> int:
        f = self._ensure_file_open()

        if "r" in self._mode and "w" not in self._mode and "a" not in self._mode:
            raise IOError(f"Cannot write in read-only mode: {self._mode}")

        total_written = 0
        remaining = data

        while remaining:
            current_segment = self.active_segment

            if current_segment is None:
                raise RuntimeError("No active segment available for writing.")

            space_left = self._max_size - current_segment.size

            if space_left <= 0:
                self._rollover()

                continue

            temp_chunk = remaining[:space_left]
            temp_written = f.write(temp_chunk)

            total_written += temp_written
            remaining = remaining[temp_written:]

            if current_segment.size >= self._max_size and remaining:
                self._rollover()

        return total_written

    def read(self, size: int = -1) -> bytes:
        f = self._ensure_file_open()

        if "w" in self._mode and "r" not in self._mode and "a" not in self._mode:
            raise IOError(f"Cannot read in write-only mode: {self._mode}")

        total_read = b""
        bytes_to_read = size

        while bytes_to_read != 0:
            current_segment = self.active_segment

            if current_segment is None:
                break

            bytes_left_in_segment = current_segment.size - f.tell()

            if bytes_left_in_segment <= 0:
                if self._current_segment_index + 1 < len(self._segments):
                    self.close()

                    self._current_segment_index += 1
                    current_segment = self.active_segment

                    if current_segment is None:
                        break

                    self._file_handle = open(current_segment.path, self._mode)
                    f = self._file_handle

                    continue
                break

            chunk_size = bytes_left_in_segment if bytes_to_read == -1 else min(bytes_left_in_segment, bytes_to_read)

            if chunk_size <= 0:
                break

            chunk = f.read(chunk_size)
            total_read += chunk

            if bytes_to_read != -1:
                bytes_to_read -= len(chunk)

            if len(chunk) < chunk_size:
                break

        return total_read

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        self._ensure_file_open()

        if whence == SEEK_SET:
            target_global_offset = offset

        elif whence == SEEK_CUR:
            current_global_offset = self._get_global_offset_from_current_position()
            target_global_offset = current_global_offset + offset

        elif whence == SEEK_END:
            total_log_size = sum(s.size for s in self._segments)
            target_global_offset = total_log_size + offset

        else:
            raise ValueError(f"Invalid whence value: {whence}")

        return self._set_position_from_global_offset(target_global_offset)

    def tell(self) -> int:
        self._ensure_file_open()

        return self._get_global_offset_from_current_position()

    def close(self) -> None:
        if self._file_handle:
            self._file_handle.close()
            self._file_handle = None

    @property
    def closed(self) -> bool:
        return self._file_handle is None or self._file_handle.closed

    def __enter__(self) -> Self:
        if self._file_handle:
            self.close()

        if not self._segments:
            if "r" in self._mode:
                raise FileNotFoundError("No segments available to open for reading.")

            if "a" in self._mode or "w" in self._mode:
                self._bump_new_segment()
            else:
                raise RuntimeError("No segments found and no suitable mode to create one.")

        if "a" in self._mode:
            self._set_position_from_global_offset(sum(s.size for s in self._segments))
        else:
            self._set_position_from_global_offset(0)

        return self

    def __exit__(self, *_: object) -> None:
        self.close()
