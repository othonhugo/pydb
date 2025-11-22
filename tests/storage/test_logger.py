import os
from pathlib import Path

import pytest

from mydb.file import MonolithicFile
from mydb.index.in_memory import InMemoryIndex
from mydb.interface import File
from mydb.storage.logger import (
    AppendOnlyLogHeader,
    AppendOnlyLogOperation,
    AppendOnlyLogPayload,
    AppendOnlyLogStorage,
    LogCorruptedError,
    LogKeyNotFoundError,
)

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
    return tmp_path / "mydb_test.db"


@pytest.fixture
def log_file(log_filepath: Path) -> File:
    return MonolithicFile(log_filepath.name, log_filepath.parent, "a+b")


@pytest.fixture
def in_memory_index() -> InMemoryIndex:
    """Returns a new, empty InMemoryIndex instance for each test."""

    return InMemoryIndex()


@pytest.fixture
def log_storage(log_file: File, in_memory_index: InMemoryIndex) -> AppendOnlyLogStorage:
    return AppendOnlyLogStorage(log_file, in_memory_index)


@pytest.mark.parametrize("key, value", BASE_SCENARIOS)
def test_set_then_get_returns_value(log_storage: AppendOnlyLogStorage, key: bytes, value: bytes):
    """
    Sets a key-value pair and then immediately retrieves it.

    Verifies the most fundamental write/read cycle (the "happy path") works correctly.
    """

    # ARRANGE
    database = log_storage

    # ACT
    database.set(key, value)
    retrieved_value = database.get(key)

    # ASSERT
    assert retrieved_value == value


@pytest.mark.parametrize("key, initial_value, updated_value", UPDATE_SCENARIOS)
def test_get_returns_latest_value_for_key(log_storage: AppendOnlyLogStorage, key: bytes, initial_value: bytes, updated_value: bytes):
    """
    Retrieves a key that has been set multiple times.

    Confirms the "last-write-wins" semantics, ensuring the storage returns the value
    from the most recent SET operation for a given key.
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
def test_deleted_key_is_inaccessible(log_storage: AppendOnlyLogStorage, key: bytes, value: bytes):
    """
    Tries to GET a key after it has been deleted.

    Ensures the DELETE operation correctly marks a key as unavailable,
    causing subsequent GET operations to fail as expected.
    """

    # ARRANGE
    database = log_storage
    database.set(key, value)

    # ACT
    database.delete(key)

    # ASSERT
    with pytest.raises(LogKeyNotFoundError) as exc_info:
        database.get(key)

    assert exc_info.value.key == key


@pytest.mark.parametrize("key, initial_value, new_value", UPDATE_SCENARIOS)
def test_set_after_delete_restores_key(log_storage: AppendOnlyLogStorage, key: bytes, initial_value: bytes, new_value: bytes):
    """
    Sets a new value for a key that was previously deleted.

    Verifies that a key can be "resurrected" with a new value after a DELETE operation.
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
def test_delete_nonexistent_key_does_not_error(log_storage: AppendOnlyLogStorage, key: bytes, _: bytes):
    """
    Calls DELETE on a key that has never been set.

    Confirms that deleting a non-existent key is a safe, idempotent operation
    that does not raise an error.
    """

    # ARRANGE
    database = log_storage

    # ACT & ASSERT
    try:
        database.delete(key)
    except Exception as e:
        pytest.fail(f"Deleting a non-existent key raised an unexpected exception: {e}")

    with pytest.raises(LogKeyNotFoundError):
        log_storage.get(key)


@pytest.mark.parametrize("key, value", BASE_SCENARIOS)
def test_data_persists_across_instances(log_file: File, in_memory_index: InMemoryIndex, key: bytes, value: bytes):
    """
    Writes data with one storage instance, then reads it with a new, separate instance.

    Validates that data is correctly persisted to the file and is not just held
    in memory, ensuring durability.
    """

    # ARRANGE:
    writer_instance = AppendOnlyLogStorage(file=log_file, index=in_memory_index)
    reader_instance = AppendOnlyLogStorage(file=log_file, index=in_memory_index)

    # ACT
    writer_instance.set(key, value)
    retrieved_value = reader_instance.get(key)

    # ASSERT
    assert retrieved_value == value


@pytest.mark.parametrize("key, value", BASE_SCENARIOS)
def test_get_unknown_key_raises_error(log_storage: AppendOnlyLogStorage, key: bytes, value: bytes):
    """
    Tries to GET a key that was never written to the log.

    Verifies that accessing a non-existent key raises the specific `LogKeyNotFoundError`.
    """

    # ARRANGE
    database = log_storage
    database.set(key, value)

    unknown_key = b"this-key-was-never-written"

    assert unknown_key != key

    # ACT & ASSERT
    with pytest.raises(LogKeyNotFoundError) as exc_info:
        database.get(unknown_key)

    assert exc_info.value.key == unknown_key


@pytest.mark.parametrize("unknown_key, _", BASE_SCENARIOS)
def test_get_from_empty_log_raises_error(log_storage: AppendOnlyLogStorage, unknown_key: bytes, _: bytes):
    """
    Tries to GET a key from an empty (zero-byte) log file.

    Confirms that the system behaves correctly when the log file exists but is empty,
    raising `LogKeyNotFoundError`.
    """

    # ARRANGE
    database = log_storage

    # ACT & ASSERT
    with pytest.raises(LogKeyNotFoundError) as exc_info:
        database.get(unknown_key)

    assert exc_info.value.key == unknown_key


def test_get_from_missing_file_raises_error(log_filepath: Path, in_memory_index: InMemoryIndex):
    """
    Attempts to GET a key when the underlying log file does not exist.

    Verifies that the storage raises `FileNotFoundError`, correctly propagating
    the underlying OS-level error.
    """

    pytest.skip()

    # ARRANGE
    key = b"any_key"

    log_file = MonolithicFile(log_filepath.name, log_filepath.parent)
    database = AppendOnlyLogStorage(file=log_file, index=in_memory_index)

    in_memory_index.set(key, 0)

    assert log_filepath.exists()

    if log_filepath.exists():
        log_filepath.rmdir()

    # ACT & ASSERT
    with pytest.raises(FileNotFoundError):
        database.get(key)


def test_truncated_header_raises_corruption_error(log_file: File, in_memory_index: InMemoryIndex):
    """
    Reads a log file where a record header is incomplete.

    Ensures the system detects a partially written header and raises `LogCorruptedError`
    to prevent processing invalid data.
    """

    pytest.skip()

    # ARRANGE
    header = AppendOnlyLogHeader(AppendOnlyLogOperation.SET, key_size=10, value_size=20)
    header_bytes = header.to_bytes()

    assert len(header_bytes) >= 5

    truncated_header = header_bytes[:-5]

    with log_file as f:
        f.write(truncated_header)

    # ACT & ASSERT
    with pytest.raises(LogCorruptedError):
        AppendOnlyLogStorage(file=log_file, index=in_memory_index)


def test_truncated_payload_raises_corruption_error(log_file: File, in_memory_index: InMemoryIndex):
    """
    Reads a log file where a record's payload is shorter than specified in its header.

    Confirms that the system validates payload length against the header,
    raising `LogCorruptedError` on a mismatch.
    """

    pytest.skip()

    # ARRANGE
    key, value = b"my-key", b"my-value"

    header = AppendOnlyLogHeader(AppendOnlyLogOperation.SET, key_size=len(key), value_size=len(value))
    payload = AppendOnlyLogPayload(key=key, value=value)

    header_bytes = header.to_bytes()
    payload_bytes = payload.to_bytes()

    assert len(payload_bytes) >= 5

    with open(str(log_filepath), "wb") as f:
        f.write(header_bytes)
        f.write(payload_bytes[:-5])

    # ACT & ASSERT
    with pytest.raises(LogCorruptedError):
        AppendOnlyLogStorage(file=log_file, index=in_memory_index)


def test_garbage_data_raises_corruption_error(log_file: File, in_memory_index: InMemoryIndex):
    """
    Reads a log file containing random, invalid binary data instead of structured records.

    Tests the system's resilience to severe corruption, ensuring it raises
    `LogCorruptedError` instead of crashing or returning incorrect data.
    """

    pytest.skip()

    # ARRANGE: Write 100 bytes of random noise to the file.
    with open(str(log_filepath), "wb") as f:
        f.write(os.urandom(100))

    # ACT & ASSERT
    with pytest.raises(LogCorruptedError):
        AppendOnlyLogStorage(file=log_file, index=in_memory_index)


def test_multiple_keys_store_and_retrieve_correctly(log_storage: AppendOnlyLogStorage):
    """
    Writes multiple distinct key-value pairs and verifies each one can be retrieved.

    Ensures that the storage can manage multiple keys simultaneously without interference.
    """

    # ARRANGE
    database = log_storage
    sequential_items = {f"key-{n}".encode(): f"value-{n}".encode() for n in range(100)}

    for key, value in sequential_items.items():
        database.set(key, value)

    # ACT & ASSERT
    for key, expected_value in sequential_items.items():
        assert database.get(key) == expected_value


def test_key_operations_do_not_affect_others(log_storage: AppendOnlyLogStorage):
    """
    Performs operations (SET, DELETE) on one key and then verifies the value of another.

    Guarantees that operations are correctly scoped to a single key and have no side
    effects on other keys in the log.
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


def test_directory_as_filepath_raises_error(tmp_path: Path, in_memory_index: InMemoryIndex):
    """
    Initializes the storage engine with a path that points to a directory.

    Confirms that the system fails gracefully with an `IsADirectoryError` when
    attempting file operations on a directory.
    """

    pytest.skip()

    # ARRANGE & ACT & ASSERT
    invalid_file: File

    with pytest.raises(IsADirectoryError):
        database = AppendOnlyLogStorage(file=invalid_file, index=in_memory_index)
        database.set(b"some_key", b"some_value")


def test_interleaved_operations_maintain_key_integrity(log_storage: AppendOnlyLogStorage):
    """
    Performs a sequence of mixed SET and DELETE operations on multiple keys.

    Verifies that the final state of each key is correct, proving the engine can
    handle a complex, realistic workload.
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


def test_partial_write_does_not_corrupt_existing_data(log_file: File, in_memory_index: InMemoryIndex):
    """
    Simulates a crash by writing a valid log followed by a partial, incomplete record.

    Verifies that a GET on a key from the valid part of the log still succeeds,
    showing resilience to write failures.
    """

    pytest.skip()

    # ARRANGE
    valid_key = b"good_key"
    valid_value = b"good_value"

    crashed_instance = AppendOnlyLogStorage(file=log_filepath, index=in_memory_index)

    # ACT & ASSERT
    crashed_instance.set(valid_key, valid_value)

    with open(log_filepath, "ab") as f:
        f.write(b"\x00\x01")  # Garbage/partial header

    recovered_instance = AppendOnlyLogStorage(file=log_filepath, index=in_memory_index)

    try:
        retrieved_value = recovered_instance.get(valid_key)

        assert retrieved_value == valid_value
    except Exception as e:
        pytest.fail(f"GET operation failed after partial write: {e}")
