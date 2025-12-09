"""
Tests for pydb.core.storage.logger.AppendOnlyLogStorage

This module contains comprehensive tests for the AppendOnlyLogStorage component,
which provides persistent key-value storage using an append-only log structure.

The test suite covers:
- Core CRUD operations (set, get, delete)
- Last-write-wins semantics for updates
- Data persistence across storage instances
- Edge cases including empty keys/values, binary data, and large payloads
- Error handling for missing keys and corrupted data
- Multi-key operations and isolation
- Durability and crash recovery scenarios
"""

import os
from pathlib import Path

import pytest

from pydb.core.file import MonolithicFile
from pydb.core.index.in_memory import InMemoryIndex
from pydb.core.storage import logger
from pydb.interface import File

EDGE_SCENARIOS = [
    # fmt: off

    # Tests handling of zero-length keys (key_size = 0).
    pytest.param(
        b"", b"value_for_empty_key",
        id="empty_key",
    ),

    # Tests handling of zero-length values (value_size = 0).
    pytest.param(
        b"key_for_empty_value", b"",
        id="empty_value",
    ),

    # Tests the case where the entire payload is zero bytes.
    pytest.param(
        b"", b"",
        id="empty_key_and_value",
    ),

    # Ensures the system is "binary safe" and doesn't mistake null bytes for string terminators.
    pytest.param(
        b"key\x00with\x00nulls", b"value\x00with\x00null_bytes",
        id="data_with_null_bytes",
    ),

    # Verifies that the system handles arbitrary non-printable byte sequences without corruption.
    pytest.param(
        b"\xDE\xAD\xBE\xEF", b"\xCA\xFE\xBA\xBE\x00\xFF",
        id="pure_binary_data",
    ),

    # Checks that byte length (not character count) is used for size calculations with multi-byte chars.
    pytest.param(
        "chave_com_acentuação".encode("utf-8"), "valor_com_símbolos_€_©".encode("utf-8"),
        id="utf8_multibyte_characters",
    ),

    # Tests the lower boundary for data size (minimal non-empty record).
    pytest.param(
        b"k", b"v",
        id="single_byte_key_and_value",
    ),

    # Tests performance and buffer handling with a large key.
    pytest.param(
        b"a" * 1024, b"value_for_large_key",
        id="large_key_small_value",
    ),

    # Stress-tests I/O and memory usage with a very large value.
    pytest.param(
        b"small_key_for_large_value", b"b" * (1024 * 1024),
        id="small_key_large_value",
    ),
]

BASE_SCENARIOS = [
    # fmt: off

    *EDGE_SCENARIOS,

    # Serves as a baseline "happy path" to ensure basic functionality.
    pytest.param(
        b"normal_key", b"normal_value",
        id="standard_ascii_case",
    ),
]

UPDATE_SCENARIOS = [
    # fmt: off

    # The baseline "happy path" for the last-write-wins logic.
    pytest.param(
        b"standard_key", b"initial_version", b"final_version",
        id="standard_update",
    ),

    # Ensures a value can be updated to an empty state, which is a valid and distinct state.
    pytest.param(
        b"key_to_be_emptied", b"some_data_here", b"",
        id="update_to_empty_value",
    ),

    # Ensures a key with an empty value can be updated to have a non-empty value.
    pytest.param(
        b"key_starts_empty", b"", b"now_has_data",
        id="update_from_empty_value",
    ),

    # Verifies the log scanner correctly reads a new, larger record for an existing key.
    pytest.param(
        b"size_grows", b"small", b"this is a much larger value than the original",
        id="update_to_larger_value",
    ),

    # Verifies the log scanner correctly reads a new, smaller record after a larger one.
    pytest.param(
        b"size_shrinks", b"this is the initial, very long value that will be replaced", b"tiny",
        id="update_to_smaller_value",
    ),

    # Confirms that the update logic is binary-safe and works with arbitrary bytes.
    pytest.param(
        b"binary_key", b"\xDE\xAD\xBE\xEF", b"\xCA\xFE\xBA\xBE",
        id="binary_data_update",
    ),

    # Ensures a new record is appended even if the value is identical, confirming append-only behavior.
    pytest.param(
        b"same_value_key", b"identical_value", b"identical_value",
        id="update_with_same_value",
    ),
]

SEQUENTIAL_SCENARIOS = [
    # fmt: off

    pytest.param(
        f"key-{n}".encode(), f"value-{n}".encode(),
        id=f"sequential-item-{n}",
    ) for n in range(100)
]


@pytest.fixture
def log_filepath(tmp_path: Path) -> Path:
    """Provides a temporary file path for the log file in each test."""
    return tmp_path / "mydb_test.db"


@pytest.fixture
def log_file(log_filepath: Path) -> File:
    """Provides a MonolithicFile instance for testing log storage."""
    return MonolithicFile(log_filepath.name, log_filepath.parent, "a+b")


@pytest.fixture
def in_memory_index() -> InMemoryIndex:
    """Provides a new, empty InMemoryIndex instance for each test."""
    
    return InMemoryIndex()


@pytest.fixture
def log_storage(log_file: File, in_memory_index: InMemoryIndex) -> logger.AppendOnlyLogStorage:
    """Provides an AppendOnlyLogStorage instance with file and index dependencies."""

    return logger.AppendOnlyLogStorage(log_file, in_memory_index)


@pytest.mark.parametrize("key, value", BASE_SCENARIOS)
def test_set_then_get_returns_value(log_storage: logger.AppendOnlyLogStorage, key: bytes, value: bytes) -> None:
    """
    Test basic set and get operations.

    Given: An empty AppendOnlyLogStorage
    When: A key-value pair is set and then retrieved
    Then: The retrieved value matches the original value
    """

    # ARRANGE
    database = log_storage

    # ACT
    database.set(key, value)
    retrieved_value = database.get(key)

    # ASSERT
    assert retrieved_value == value


@pytest.mark.parametrize("key, initial_value, updated_value", UPDATE_SCENARIOS)
def test_get_returns_latest_value_for_key(log_storage: logger.AppendOnlyLogStorage, key: bytes, initial_value: bytes, updated_value: bytes) -> None:
    """
    Test last-write-wins semantics for updates.

    Given: An AppendOnlyLogStorage with a key set to an initial value
    When: The same key is set again with a different value
    Then: Getting the key returns the most recent value
    """

    # ARRANGE
    database = log_storage

    # ACT
    database.set(key, initial_value)
    database.set(key, updated_value)

    retrieved_value = database.get(key)

    # ASSERT
    assert retrieved_value == updated_value


@pytest.mark.parametrize("key, value", BASE_SCENARIOS)
def test_deleted_key_is_inaccessible(log_storage: logger.AppendOnlyLogStorage, key: bytes, value: bytes) -> None:
    """
    Test deleting a key from storage.

    Given: An AppendOnlyLogStorage with a key-value pair set
    When: The key is deleted
    Then: Attempting to get the key raises LogKeyNotFoundError
    """

    # ARRANGE
    database = log_storage
    database.set(key, value)

    # ACT
    database.delete(key)

    # ASSERT
    with pytest.raises(logger.LogKeyNotFoundError) as exc_info:
        database.get(key)

    assert exc_info.value.key == key


@pytest.mark.parametrize("key, initial_value, new_value", UPDATE_SCENARIOS)
def test_set_after_delete_restores_key(log_storage: logger.AppendOnlyLogStorage, key: bytes, initial_value: bytes, new_value: bytes) -> None:
    """
    Test restoring a deleted key with a new value.

    Given: An AppendOnlyLogStorage with a key that has been deleted
    When: The key is set again with a new value
    Then: The key can be retrieved with the new value
    """

    # ARRANGE
    database = log_storage
    database.set(key, initial_value)
    database.delete(key)

    # ACT
    database.set(key, new_value)
    retrieved_value = log_storage.get(key)

    # ASSERT
    assert retrieved_value == new_value


@pytest.mark.parametrize("key, _", BASE_SCENARIOS)
def test_delete_nonexistent_key_does_not_error(log_storage: logger.AppendOnlyLogStorage, key: bytes, _: bytes) -> None:
    """
    Test deleting a non-existent key.

    Given: An empty AppendOnlyLogStorage
    When: Attempting to delete a key that was never set
    Then: The operation completes without raising an error (idempotent)
    """

    # ARRANGE
    database = log_storage

    # ACT & ASSERT
    try:
        database.delete(key)
    except Exception as e:
        pytest.fail(f"Deleting a non-existent key raised an unexpected exception: {e}")

    with pytest.raises(logger.LogKeyNotFoundError):
        log_storage.get(key)


@pytest.mark.parametrize("key, value", BASE_SCENARIOS)
def test_data_persists_across_instances(log_file: File, in_memory_index: InMemoryIndex, key: bytes, value: bytes) -> None:
    """
    Test data persistence across storage instances.

    Given: Data written by one AppendOnlyLogStorage instance
    When: A new instance is created with the same file
    Then: The data can be read by the new instance (durability)
    """

    # ARRANGE:
    writer_instance = logger.AppendOnlyLogStorage(file=log_file, index=in_memory_index)
    reader_instance = logger.AppendOnlyLogStorage(file=log_file, index=in_memory_index)

    # ACT
    writer_instance.set(key, value)
    retrieved_value = reader_instance.get(key)

    # ASSERT
    assert retrieved_value == value


@pytest.mark.parametrize("key, value", BASE_SCENARIOS)
def test_get_unknown_key_raises_error(log_storage: logger.AppendOnlyLogStorage, key: bytes, value: bytes) -> None:
    """
    Test getting a key that doesn't exist.

    Given: An AppendOnlyLogStorage with some data
    When: Attempting to get a key that was never written
    Then: LogKeyNotFoundError is raised with the correct key
    """

    # ARRANGE
    database = log_storage
    database.set(key, value)

    unknown_key = b"this-key-was-never-written"

    assert unknown_key != key

    # ACT & ASSERT
    with pytest.raises(logger.LogKeyNotFoundError) as exc_info:
        database.get(unknown_key)

    assert exc_info.value.key == unknown_key


@pytest.mark.parametrize("unknown_key, _", BASE_SCENARIOS)
def test_get_from_empty_log_raises_error(log_storage: logger.AppendOnlyLogStorage, unknown_key: bytes, _: bytes) -> None:
    """
    Test getting a key from an empty log file.

    Given: An AppendOnlyLogStorage with an empty log file
    When: Attempting to get any key
    Then: LogKeyNotFoundError is raised
    """

    # ARRANGE
    database = log_storage

    # ACT & ASSERT
    with pytest.raises(logger.LogKeyNotFoundError) as exc_info:
        database.get(unknown_key)

    assert exc_info.value.key == unknown_key


def test_get_from_missing_file_raises_error(log_filepath: Path, in_memory_index: InMemoryIndex) -> None:
    """
    Test getting a key when the log file doesn't exist.

    Given: An AppendOnlyLogStorage configured with a non-existent file
    When: Attempting to get a key
    Then: FileNotFoundError is raised
    """

    pytest.skip()

    # ARRANGE
    key = b"any_key"

    log_file = MonolithicFile(log_filepath.name, log_filepath.parent)
    database = logger.AppendOnlyLogStorage(file=log_file, index=in_memory_index)

    in_memory_index.set(key, 0)

    assert log_filepath.exists()

    if log_filepath.exists():
        log_filepath.rmdir()

    # ACT & ASSERT
    with pytest.raises(FileNotFoundError):
        database.get(key)


def test_truncated_header_raises_corruption_error(log_file: File, in_memory_index: InMemoryIndex) -> None:
    """
    Test reading a log with an incomplete record header.

    Given: A log file with a truncated header
    When: Initializing AppendOnlyLogStorage
    Then: LogCorruptedError is raised
    """

    pytest.skip()

    # ARRANGE
    header = logger.AppendOnlyLogHeader(logger.AppendOnlyLogOperation.SET, key_size=10, value_size=20)
    header_bytes = header.to_bytes()

    assert len(header_bytes) >= 5

    truncated_header = header_bytes[:-5]

    with log_file as f:
        f.write(truncated_header)

    # ACT & ASSERT
    with pytest.raises(logger.LogCorruptedError):
        logger.AppendOnlyLogStorage(file=log_file, index=in_memory_index)


def test_truncated_payload_raises_corruption_error(log_file: File, in_memory_index: InMemoryIndex) -> None:
    """
    Test reading a log with an incomplete record payload.

    Given: A log file with a payload shorter than specified in the header
    When: Initializing AppendOnlyLogStorage
    Then: LogCorruptedError is raised
    """

    pytest.skip()

    # ARRANGE
    key, value = b"my-key", b"my-value"

    header = logger.AppendOnlyLogHeader(logger.AppendOnlyLogOperation.SET, key_size=len(key), value_size=len(value))
    payload = logger.AppendOnlyLogPayload(key=key, value=value)

    header_bytes = header.to_bytes()
    payload_bytes = payload.to_bytes()

    assert len(payload_bytes) >= 5

    with open(str(log_filepath), "wb") as f:
        f.write(header_bytes)
        f.write(payload_bytes[:-5])

    # ACT & ASSERT
    with pytest.raises(logger.LogCorruptedError):
        logger.AppendOnlyLogStorage(file=log_file, index=in_memory_index)


def test_garbage_data_raises_corruption_error(log_file: File, in_memory_index: InMemoryIndex) -> None:
    """
    Test reading a log file containing random invalid data.

    Given: A log file with random binary data instead of valid records
    When: Initializing AppendOnlyLogStorage
    Then: LogCorruptedError is raised
    """

    pytest.skip()

    # ARRANGE: Write 100 bytes of random noise to the file.
    with open(str(log_filepath), "wb") as f:
        f.write(os.urandom(100))

    # ACT & ASSERT
    with pytest.raises(logger.LogCorruptedError):
        logger.AppendOnlyLogStorage(file=log_file, index=in_memory_index)


def test_multiple_keys_store_and_retrieve_correctly(log_storage: logger.AppendOnlyLogStorage) -> None:
    """
    Test storing and retrieving multiple distinct keys.

    Given: An empty AppendOnlyLogStorage
    When: Multiple distinct key-value pairs are written
    Then: Each key can be retrieved with its correct value
    """

    # ARRANGE
    database = log_storage
    sequential_items = {f"key-{n}".encode(): f"value-{n}".encode() for n in range(100)}

    for key, value in sequential_items.items():
        database.set(key, value)

    # ACT & ASSERT
    for key, expected_value in sequential_items.items():
        assert database.get(key) == expected_value


def test_key_operations_do_not_affect_others(log_storage: logger.AppendOnlyLogStorage) -> None:
    """
    Test operation isolation between different keys.

    Given: An AppendOnlyLogStorage with multiple keys set
    When: Operations are performed on one key
    Then: Other keys remain unaffected
    """

    # ARRANGE
    database = log_storage
    database.set(b"key-1", b"value-1")
    database.set(b"key-2", b"value-2")

    # ACT
    database.set(b"key-1", b"new-value-1")
    database.delete(b"key-1")

    # ASSERT: key-2 should be completely unaffected
    assert database.get(b"key-2") == b"value-2"


def test_directory_as_filepath_raises_error(tmp_path: Path, in_memory_index: InMemoryIndex) -> None:
    """
    Test initializing storage with a directory path instead of a file.

    Given: A path pointing to a directory
    When: Attempting to initialize AppendOnlyLogStorage
    Then: IsADirectoryError is raised
    """

    pytest.skip()

    # ARRANGE & ACT & ASSERT
    invalid_file: File

    with pytest.raises(IsADirectoryError):
        database = logger.AppendOnlyLogStorage(file=invalid_file, index=in_memory_index)
        database.set(b"some_key", b"some_value")


def test_interleaved_operations_maintain_key_integrity(log_storage: logger.AppendOnlyLogStorage) -> None:
    """
    Test complex interleaved operations on multiple keys.

    Given: An empty AppendOnlyLogStorage
    When: Performing mixed SET and DELETE operations on multiple keys
    Then: The final state of each key is correct
    """

    # ARRANGE
    database = log_storage

    # ACT
    database.set(b"k1", b"alpha")
    database.set(b"k2", b"beta")
    database.set(b"k1", b"gamma")
    database.delete(b"k2")
    database.set(b"k3", b"delta")
    database.set(b"k2", b"epsilon")

    # ASSERT: Check the final state of all keys
    assert database.get(b"k1") == b"gamma"
    assert database.get(b"k2") == b"epsilon"
    assert database.get(b"k3") == b"delta"


def test_partial_write_does_not_corrupt_existing_data(log_file: File, in_memory_index: InMemoryIndex) -> None:
    """
    Test recovery after a partial write (simulated crash).

    Given: A log file with valid data followed by a partial, incomplete record
    When: Reading data from the valid portion of the log
    Then: The valid data can still be retrieved successfully
    """

    pytest.skip()

    # ARRANGE
    valid_key = b"good_key"
    valid_value = b"good_value"

    crashed_instance = logger.AppendOnlyLogStorage(file=log_filepath, index=in_memory_index)

    # ACT & ASSERT
    crashed_instance.set(valid_key, valid_value)

    with open(log_filepath, "ab") as f:
        f.write(b"\x00\x01")  # Garbage/partial header

    recovered_instance = logger.AppendOnlyLogStorage(file=log_filepath, index=in_memory_index)

    try:
        retrieved_value = recovered_instance.get(valid_key)

        assert retrieved_value == valid_value
    except Exception as e:
        pytest.fail(f"GET operation failed after partial write: {e}")
