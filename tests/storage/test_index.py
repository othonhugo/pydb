"""
Tests for pydb.core.index.InMemoryIndex

This module contains comprehensive tests for the InMemoryIndex component,
which provides an in-memory key-value index mapping binary keys to integer offsets.

The test suite covers:
- Core CRUD operations (set, get, has, delete)
- Edge cases including empty keys, binary data, and large keys
- Update semantics (last-write-wins)
- Error handling for non-existent keys
- Full lifecycle operations across various key formats
"""

import pytest

from pydb.core.index import InMemoryIndex, InMemoryIndexKeyNotFoundError

EDGE_SCENARIOS = [
    # fmt: off

    # A zero-length key, the most fundamental edge case.
    pytest.param(
        b"",
        id="empty-key"
    ),

    # A key containing only a single space.
    pytest.param(
        b" ",
        id="single-space-key"
    ),

    # Ensures leading/trailing whitespace is treated as part of the key, not trimmed.
    pytest.param(
        b"  leading-and-trailing-spaces  ",
        id="key-with-whitespace"
    ),

    # Proves the system is "binary safe" by handling null bytes correctly.
    pytest.param(
        b"key\x00with\x00nulls",
        id="key-with-null-bytes"
    ),

    # A key made of arbitrary non-printable bytes to test "8-bit clean" handling.
    pytest.param(
        b"\xde\xad\xbe\xef",
        id="purely-binary-key"
    ),

    # Verifies that control characters like newlines are handled literally.
    pytest.param(
        b"key\nwith\r\nnewlines",
        id="key-with-control-chars"
    ),

    # A key with byte values outside the standard 7-bit ASCII range.
    pytest.param(
        b"\xff\xfe\xfd",
        id="key-with-high-bytes"
    ),

    # The smallest possible non-empty key.
    pytest.param(
        b"A",
        id="single-byte-key"
    ),

    # A large key (4KB) to test for performance or buffer-related issues.
    pytest.param(
        b"A" * 4096,
        id="long-key-4kb"
    ),

    # A key containing multi-byte UTF-8 characters.
    pytest.param(
        "chave-com-acentuação-ç".encode("utf-8"),
        id="utf8-encoded-key"
    ),

    # A key with various symbols that might be special in other parsing contexts.
    pytest.param(
        b'key-with-"quotes"-and-symbols/\\!@#$%',
        id="key-with-special-symbols"
    ),
]

# A constant for testing large offsets, representing the 4GB file size boundary.
LARGE_OFFSET = 2**32

# A comprehensive collection of scenarios combining edge-case keys with edge-case offsets.
BASE_SCENARIOS = [
    # fmt: off

    # A standard key at offset 0, the very beginning of the file.
    pytest.param(
        b"standard-key", 0,
        id="standard-key-offset-zero"
    ),

    # A standard key with a very large offset to test handling of large files.
    pytest.param(
        b"standard-key", LARGE_OFFSET,
        id="standard-key-large-offset"
    ),

    # A zero-length key, the most fundamental edge case.
    pytest.param(
        b"", 12345,
        id="empty-key"
    ),

    # A key containing only a single space.
    pytest.param(
        b" ", 12345,
        id="single-space-key"
    ),

    # Ensures leading/trailing whitespace is treated as part of the key, not trimmed.
    pytest.param(
        b"  leading-and-trailing-spaces  ", 12345,
        id="key-with-whitespace"
    ),

    # Proves the system is "binary safe" by handling null bytes correctly.
    pytest.param(
        b"key\x00with\x00nulls", 12345,
        id="key-with-null-bytes"
    ),

    # A key made of arbitrary non-printable bytes to test "8-bit clean" handling.
    pytest.param(
        b"\xde\xad\xbe\xef", 12345,
        id="purely-binary-key"
    ),

    # Verifies that control characters like newlines are handled literally.
    pytest.param(
        b"key\nwith\r\nnewlines", 12345,
        id="key-with-control-chars"
    ),

    # A key with byte values outside the standard 7-bit ASCII range.
    pytest.param(
        b"\xff\xfe\xfd", 12345,
        id="key-with-high-bytes"
    ),

    # The smallest possible non-empty key.
    pytest.param(
        b"A", 12345,
        id="single-byte-key"
    ),

    # A large key (4KB) to test for performance or buffer-related issues.
    pytest.param(b"A" * 4096, 12345,
        id="long-key-4kb"
    ),

    # A key containing multi-byte UTF-8 characters.
    pytest.param(
        "chave-com-acentuação-ç".encode("utf-8"), 12345,
        id="utf8-encoded-key"
    ),

    # A key with various symbols that might be special in other parsing contexts.
    pytest.param(
        b'key-with-"quotes"-and-symbols/\\!@#$%', 12345,
        id="key-with-special-symbols"
    ),
]

# A comprehensive collection of scenarios for testing the update (overwrite) logic.
UPDATE_SCENARIOS = [
    # fmt: off

    # The baseline "happy path" for an update, with a standard key.
    pytest.param(
        b"standard-key", 100, 500,
        id="standard-update"
    ),

    # An update where the initial record was at the very beginning of the file.
    pytest.param(
        b"key-at-zero", 0, 250,
        id="update-from-offset-zero"
    ),

    # An update to a very large offset, simulating a large log file.
    pytest.param(
        b"key-with-large-offset", 200, LARGE_OFFSET,
        id="update-to-large-offset"
    ),

    *(pytest.param(
        p.values[0],          # The key from the original scenario
        123,                  # A standard initial offset
        456,                  # A standard updated offset
        id=f"{p.id}-update"   # Append '-update' to the original ID for clarity
    ) for p in BASE_SCENARIOS),
]


@pytest.fixture
def in_memory_index() -> InMemoryIndex:
    """Returns a new, empty InMemoryIndex instance for each test."""

    return InMemoryIndex()


# Core Functionality and Lifecycle Tests


@pytest.mark.parametrize("key, offset", BASE_SCENARIOS)
def test_set_new_key_can_be_retrieved(in_memory_index: InMemoryIndex, key: bytes, offset: int) -> None:
    """
    Test setting a new key and retrieving its offset.

    Given: An empty InMemoryIndex
    When: A key is set with a specific offset
    Then: The offset can be retrieved immediately
    """

    # ARRANGE
    index = in_memory_index

    # ACT
    index.set(key, offset)
    retrieved_offset = index.get(key)

    # ASSERT
    assert retrieved_offset == offset


@pytest.mark.parametrize("key, initial_offset, updated_offset", UPDATE_SCENARIOS)
def test_set_existing_key_updates_offset(in_memory_index: InMemoryIndex, key: bytes, initial_offset: int, updated_offset: int) -> None:
    """
    Test updating an existing key with a new offset.

    Given: An InMemoryIndex with a key already set
    When: The same key is set again with a different offset
    Then: The offset is updated following last-write-wins semantics
    """

    # ARRANGE
    index = in_memory_index

    # ACT
    index.set(key, initial_offset)
    index.set(key, updated_offset)
    retrieved_offset = index.get(key)

    # ASSERT
    assert retrieved_offset == updated_offset


@pytest.mark.parametrize("key, offset", BASE_SCENARIOS)
def test_has_returns_true_for_existing_key(in_memory_index: InMemoryIndex, key: bytes, offset: int) -> None:
    """
    Test checking for the presence of an existing key.

    Given: An InMemoryIndex with a key set
    When: The has() method is called for that key
    Then: It returns True
    """

    # ARRANGE
    index = in_memory_index

    # ACT
    index.set(key, offset)

    # ASSERT
    assert index.has(key) is True


@pytest.mark.parametrize("key, offset", BASE_SCENARIOS)
def test_deleted_key_is_no_longer_accessible(in_memory_index: InMemoryIndex, key: bytes, offset: int) -> None:
    """
    Test deleting a key from the index.

    Given: An InMemoryIndex with a key set
    When: The key is deleted
    Then: The key is no longer accessible via get() and has() returns False
    """

    # ARRANGE
    index = in_memory_index

    # ACT
    index.set(key, offset)
    index.delete(key)

    # ASSERT
    assert index.has(key) is False

    with pytest.raises(InMemoryIndexKeyNotFoundError) as exc_info:
        index.get(key)

    assert exc_info.value.key == key


@pytest.mark.parametrize("key, _", BASE_SCENARIOS)
def test_get_nonexistent_key_raises_error(in_memory_index: InMemoryIndex, key: bytes, _: int) -> None:
    """
    Test getting a non-existent key.

    Given: An empty InMemoryIndex
    When: Attempting to get a key that was never set
    Then: InMemoryIndexKeyNotFoundError is raised with the correct key
    """

    # ARRANGE
    index = in_memory_index

    # ACT & ASSERT
    with pytest.raises(InMemoryIndexKeyNotFoundError) as exc_info:
        index.get(key)

    assert exc_info.value.key == key


@pytest.mark.parametrize("key, _", BASE_SCENARIOS)
def test_has_returns_false_for_nonexistent_key(in_memory_index: InMemoryIndex, key: bytes, _: int) -> None:
    """
    Test checking for the presence of a non-existent key.

    Given: An empty InMemoryIndex
    When: The has() method is called for a key that was never set
    Then: It returns False
    """

    # ARRANGE
    index = in_memory_index

    # ACT & ASSERT
    assert index.has(key) is False


@pytest.mark.parametrize("key, _", BASE_SCENARIOS)
def test_delete_nonexistent_key_is_silent_and_idempotent(in_memory_index: InMemoryIndex, key: bytes, _: int) -> None:
    """
    Test deleting a non-existent key.

    Given: An empty InMemoryIndex
    When: Attempting to delete a key that was never set
    Then: The operation completes without raising an error (idempotent)
    """

    # ARRANGE
    index = in_memory_index

    # ACT & ASSERT
    try:
        index.delete(key)
    except Exception as e:
        pytest.fail(f"Deleting a non-existent key raised an unexpected exception: {e}")


@pytest.mark.parametrize("key, initial_offset, updated_offset", UPDATE_SCENARIOS)
def test_lifecycle_with_edge_case_keys(in_memory_index: InMemoryIndex, key: bytes, initial_offset: int, updated_offset: int) -> None:
    """
    Test full lifecycle operations with edge-case keys.

    Given: An empty InMemoryIndex and various edge-case keys (empty, binary, large)
    When: Performing set, update, delete, and get operations in sequence
    Then: All operations complete correctly for all key formats
    """

    # ARRANGE
    index = in_memory_index

    # ACT: Set the key for the first time.
    index.set(key, initial_offset)

    # ASSERT: Verify the key now exists and points to the correct offset.
    assert index.has(key) is True
    assert index.get(key) == initial_offset

    # ACT: Set the same key again with a new offset.
    index.set(key, updated_offset)

    # ASSERT: Verify the offset has been updated correctly (last-write-wins).
    assert index.get(key) == updated_offset

    # ACT: Delete the key.
    index.delete(key)

    # ASSERT: Verify the key is no longer present.
    assert index.has(key) is False

    # ACT & ASSERT: Verify that attempting to get the deleted key raises the correct error.
    with pytest.raises(InMemoryIndexKeyNotFoundError) as exc_info:
        index.get(key)

    assert exc_info.value.key == key
