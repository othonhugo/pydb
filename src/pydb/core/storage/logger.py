from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from os import SEEK_END
from struct import Struct
from typing import Self

from pydb import config, interface


class AppendOnlyLogOperation(IntEnum):
    """Enumeration of log operation types."""

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
    """Header for an append-only log record.

    Contains metadata about the operation type and payload sizes.
    """

    STRUCT = Struct("BQQ")

    operation: AppendOnlyLogOperation
    key_size: int
    value_size: int

    @property
    def payload_size(self) -> int:
        """Calculate the total payload size.

        Returns:
            int: The sum of key_size and value_size.
        """

        return self.key_size + self.value_size

    @property
    def record_size(self) -> int:
        """Calculate the total record size including header.

        Returns:
            int: The sum of header size and payload size.
        """

        return self.STRUCT.size + self.payload_size

    def to_bytes(self) -> bytes:
        """Serialize the header to bytes.

        Returns:
            bytes: The serialized header.
        """

        return self.STRUCT.pack(self.operation.value, self.key_size, self.value_size)

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        """Deserialize a header from bytes.

        Args:
            data (bytes): The serialized header bytes.

        Raises:
            LogStorageError: If the operation value is invalid.

        Returns:
            Self: The deserialized AppendOnlyLogHeader instance.
        """

        op_value, key_size, value_size = cls.STRUCT.unpack(data)

        try:
            operation = AppendOnlyLogOperation(op_value)
        except ValueError as e:
            raise LogStorageError(e) from e

        return cls(operation=operation, key_size=key_size, value_size=value_size)


@dataclass(frozen=True)
class AppendOnlyLogPayload:
    """Payload for an append-only log record.

    Contains the key and value bytes for a log entry.
    """

    key: bytes
    value: bytes

    def to_bytes(self) -> bytes:
        """Serialize the payload to bytes.

        Returns:
            bytes: The concatenated key and value bytes.
        """

        return self.key + self.value


@dataclass(frozen=True)
class AppendOnlyLogRecord:
    """Complete append-only log record.

    Combines a header and payload to form a complete log entry.
    """

    header: AppendOnlyLogHeader
    payload: AppendOnlyLogPayload

    def to_stream(self, stream: interface.File, /) -> int:
        """Write the record to a file stream.

        Args:
            stream (interface.File): The file stream to write to.

        Returns:
            int: The total number of bytes written.
        """

        count = stream.write(self.header.to_bytes())
        count += stream.write(self.payload.to_bytes())

        return count

    @classmethod
    def from_stream(cls, stream: interface.File, /) -> Self | None:
        """Read and deserialize a record from a file stream.

        Args:
            stream (interface.File): The file stream to read from.

        Raises:
            LogCorruptedError: If the record is truncated or corrupted.

        Returns:
            Self | None: The deserialized record, or None if at EOF.
        """

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
    """Append-only log-based storage engine implementation."""

    def __init__(self, file: interface.File, index: interface.Index) -> None:
        """Initialize the append-only log storage.

        Args:
            file (interface.File): The file to use for storage.
            index (interface.Index): The index to use for key lookups.
        """
        self._file = file
        self._index = index

        with self._file:
            self._build_index()
            self._file.seek(0, SEEK_END)

    def get(self, key: bytes, /) -> bytes:
        """Retrieve the value for a key from storage.

        Args:
            key (bytes): The key to retrieve.

        Raises:
            LogKeyNotFoundError: If the key is not found.
            LogInvalidOffsetError: If the record at the offset doesn't match the key.

        Returns:
            bytes: The value associated with the key.
        """

        if not self._index.has(key):
            raise LogKeyNotFoundError(key=key)

        try:
            offset = self._index.get(key)
        except config.IndexingError:
            raise LogKeyNotFoundError(key=key) from None

        with self._file:
            record = self._load_record_at(offset)

        if record.payload.key == key:
            return record.payload.value

        self._index.delete(key)

        raise LogInvalidOffsetError(offset=offset)

    def set(self, key: bytes, value: bytes, /) -> None:
        """Store a key-value pair in the storage.

        Args:
            key (bytes): The key to store.
            value (bytes): The value to store.
        """

        with self._file:
            offset = self._append_record(AppendOnlyLogOperation.SET, key, value)

        self._index.set(key, offset)

    def delete(self, key: bytes, /) -> None:
        """Delete a key-value pair from storage.

        This operation is idempotent - deleting a non-existent key has no effect.

        Args:
            key (bytes): The key to delete.
        """

        if not self._index.has(key):
            return

        with self._file:
            self._append_record(AppendOnlyLogOperation.DELETE, key, b"")

        self._index.delete(key)

    def _build_index(self) -> None:
        """Build the index by scanning the log file.

        Reads all records from the log and updates the index accordingly.
        """

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
        """Append a record to the log file.

        Args:
            operation (AppendOnlyLogOperation): The operation type (SET or DELETE).
            key (bytes): The key for the record.
            value (bytes): The value for the record.

        Returns:
            int: The file offset where the record was written.
        """

        header = AppendOnlyLogHeader(operation=operation, key_size=len(key), value_size=len(value))
        payload = AppendOnlyLogPayload(key=key, value=value)
        record = AppendOnlyLogRecord(header=header, payload=payload)

        offset = self._file.tell()
        record.to_stream(self._file)

        return offset

    def _load_record_at(self, offset: int, /) -> AppendOnlyLogRecord:
        """Load a record from a specific offset in the log.

        Args:
            offset (int): The file offset to read from.

        Raises:
            LogInvalidOffsetError: If no valid record is found at the offset.

        Returns:
            AppendOnlyLogRecord: The record at the specified offset.
        """

        self._file.seek(offset)

        record = AppendOnlyLogRecord.from_stream(self._file)

        if record is None:
            raise LogInvalidOffsetError(offset=offset)

        return record
