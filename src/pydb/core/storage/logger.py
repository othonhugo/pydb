from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from os import SEEK_END
from struct import Struct
from typing import Self

from pydb import config, interface


class AppendOnlyLogOperation(IntEnum):
    SET = 0
    DELETE = 1


class LogStorageError(config.StorageError):
    """Base exception for log storage errors."""


class LogKeyNotFoundError(LogStorageError):
    """Raised when a key is not found in the log."""

    def __init__(self, *, key: bytes):
        self.key = key

        super().__init__(f"Key not found: {key!r}")


class LogCorruptedError(LogStorageError):
    """Raised when the log file appears to be corrupted."""

    def __init__(self, *, offset: int, cause: str | Exception | None = None):
        self.offset = offset
        self.cause = cause

        message = f"Log corrupted at offset {offset}"

        if cause:
            message += f": {cause}"

        super().__init__(message)


class LogInvalidOffsetError(LogStorageError):
    """Raised when a record cannot be found at a given offset."""

    def __init__(self, *, offset: int):
        self.offset = offset

        super().__init__(f"No valid record found at offset {offset}")


@dataclass(frozen=True)
class AppendOnlyLogHeader:
    STRUCT = Struct("BQQ")

    operation: AppendOnlyLogOperation
    key_size: int
    value_size: int

    @property
    def payload_size(self) -> int:
        return self.key_size + self.value_size

    @property
    def record_size(self) -> int:
        return self.STRUCT.size + self.payload_size

    def to_bytes(self) -> bytes:
        return self.STRUCT.pack(self.operation.value, self.key_size, self.value_size)

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        op_value, key_size, value_size = cls.STRUCT.unpack(data)

        try:
            operation = AppendOnlyLogOperation(op_value)
        except ValueError as e:
            raise LogStorageError(e) from e

        return cls(operation=operation, key_size=key_size, value_size=value_size)


@dataclass(frozen=True)
class AppendOnlyLogPayload:
    key: bytes
    value: bytes

    def to_bytes(self) -> bytes:
        return self.key + self.value


@dataclass(frozen=True)
class AppendOnlyLogRecord:
    header: AppendOnlyLogHeader
    payload: AppendOnlyLogPayload

    def to_stream(self, stream: interface.File, /) -> int:
        count = stream.write(self.header.to_bytes())
        count += stream.write(self.payload.to_bytes())

        return count

    @classmethod
    def from_stream(cls, stream: interface.File, /) -> Self | None:
        offset = stream.tell()

        if not (header_bytes := stream.read(AppendOnlyLogHeader.STRUCT.size)):
            return None

        if len(header_bytes) < AppendOnlyLogHeader.STRUCT.size:
            raise LogCorruptedError(offset=offset, cause="Truncated record header.")

        try:
            header = AppendOnlyLogHeader.from_bytes(header_bytes)

            payload_bytes = stream.read(header.payload_size)

            if len(payload_bytes) != header.payload_size:
                raise LogCorruptedError(offset=offset, cause="Truncated record payload.")

            payload_struct = Struct(f"{header.key_size}s{header.value_size}s")

            key_bytes, value_bytes = payload_struct.unpack(payload_bytes)

            payload = AppendOnlyLogPayload(key=key_bytes, value=value_bytes)

            return cls(header=header, payload=payload)
        except Exception as e:
            raise LogCorruptedError(offset=offset, cause=e) from e


class AppendOnlyLogStorage(interface.StorageEngine):
    def __init__(self, file: interface.File, index: interface.Index) -> None:
        self._file = file
        self._index = index

        with self._file:
            self._build_index()
            self._file.seek(0, SEEK_END)

    def get(self, key: bytes, /) -> bytes:
        if not self._index.has(key):
            raise LogKeyNotFoundError(key=key)

        try:
            offset = self._index.get(key)
        except config.IndexError:
            raise LogKeyNotFoundError(key=key) from None

        with self._file:
            record = self._load_record_at(offset)

        if record.payload.key == key:
            return record.payload.value

        self._index.delete(key)

        raise LogInvalidOffsetError(offset=offset)

    def set(self, key: bytes, value: bytes, /) -> None:
        with self._file:
            offset = self._append_record(AppendOnlyLogOperation.SET, key, value)

        self._index.set(key, offset)

    def delete(self, key: bytes, /) -> None:
        if not self._index.has(key):
            return

        with self._file:
            self._append_record(AppendOnlyLogOperation.DELETE, key, b"")

        self._index.delete(key)

    def _build_index(self) -> None:
        while True:
            current_offset = self._file.tell()

            record = AppendOnlyLogRecord.from_stream(self._file)

            if record is None:
                break

            record_key = record.payload.key

            match record.header.operation:
                case AppendOnlyLogOperation.DELETE:
                    self._index.delete(record_key)
                case AppendOnlyLogOperation.SET:
                    self._index.set(record_key, current_offset)

    def _append_record(self, operation: AppendOnlyLogOperation, key: bytes, value: bytes) -> int:
        header = AppendOnlyLogHeader(operation=operation, key_size=len(key), value_size=len(value))
        payload = AppendOnlyLogPayload(key=key, value=value)
        record = AppendOnlyLogRecord(header=header, payload=payload)

        offset = self._file.tell()
        record.to_stream(self._file)

        return offset

    def _load_record_at(self, offset: int, /) -> AppendOnlyLogRecord:
        self._file.seek(offset)

        record = AppendOnlyLogRecord.from_stream(self._file)

        if record is None:
            raise LogInvalidOffsetError(offset=offset)

        return record
