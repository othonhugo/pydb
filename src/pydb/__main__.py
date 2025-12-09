def main() -> None:
    """Main function to demonstrate the pydb key-value store."""
    
    from tempfile import mkdtemp
    from pydb.core import file, index, storage

    print("TEMP DIR:", dirpath := mkdtemp(prefix="pydb"))

    storage_index = index.InMemoryIndex()
    storage_file = file.MonolithicFile("test", dirpath, mode="a+b")
    storage_engine = storage.AppendOnlyLogStorage(storage_file, storage_index)

    def _set(key: bytes, value: bytes) -> None:
        storage_engine.set(key, value)

        print(f"SET: {key.decode()} = {value.decode()}")

    def _get(key: bytes) -> None:
        value = storage_engine.get(key)

        print(f"GET: {key.decode()} = {value.decode()}")

    _set(b"hello", b"world")
    _set(b"hello", b"all")
    _get(b"hello")


if __name__ == "__main__":
    main()
