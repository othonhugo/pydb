# pydb

![Status](https://img.shields.io/badge/status-in%20development-yellow)
![License](https://img.shields.io/badge/license-MIT-blue)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)

**pydb** is an educational project focused on learning the internal concepts and fundamentals of a database system. The goal is not to create a commercial product, but rather to demystify what happens "under the hood" in systems like PostgreSQL, Redis, or MongoDB by building a simplified version from scratch.

This project is designed for students and software enthusiasts who want to deepen their knowledge of data structures, algorithms, I/O operations, and system architecture.

## Project Goals

- **Educational Focus**: Understand core database concepts through hands-on implementation
- **Simplified Architecture**: Build a working database system with essential components
- **Learning by Doing**: Explore storage engines, query processing, indexing, and transaction management
- **Foundation Building**: Gain insights into how production databases work internally

## Features

The project is actively being developed with the following components:

- **Storage Engine**: Mechanisms for data persistence and retrieval
- **Query Processing**: Parsing and executing database operations
- **Indexing**: Data structures for efficient data access
- **Transaction Management**: Ensuring data consistency and integrity
- **Concurrency Control**: Handling multiple simultaneous operations
- **Recovery Mechanisms**: Data durability and crash recovery

## Getting Started

### Installation

Clone the repository and install the package:

```bash
git clone https://github.com/othonhugo/pydb.git
cd pydb
pip install -e .
```

### Quick Start

Run the example demonstration:

```bash
python3 -m pydb
```

## Usage

Here's a simple example of using **pydb** as a key-value store:

```python
from tempfile import mkdtemp
from pydb.core import file, index, storage

dirpath = mkdtemp(prefix="pydb")  # Temporary directory for storage

# Initialize the storage components
storage_index = index.InMemoryIndex()
storage_file = file.MonolithicFile("test", dirpath, mode="a+b")
storage_engine = storage.AppendOnlyLogStorage(storage_file, storage_index)

# Set key-value pairs
storage_engine.set(b"hello", b"world")
storage_engine.set(b"hello", b"all")  # Updates the previous value

# Retrieve values
value = storage_engine.get(b"hello")
print(f"Value: {value.decode()}")  # Output: Value: all
```

### Key Components

- **`InMemoryIndex`**: Maintains an in-memory index for fast key lookups
- **`MonolithicFile`**: Handles file I/O operations for data persistence
- **`AppendOnlyLogStorage`**: Implements an append-only log storage engine

This simple example demonstrates the fundamental concepts of:
- Data persistence using append-only logs
- In-memory indexing for efficient retrieval
- Key-value storage operations

### Customization

**pydb** is designed to be highly customizable through well-defined interfaces. You can implement your own storage strategies by creating custom implementations of the core abstractions:

#### **`StorageEngine` Interface**

Define how data is stored and retrieved. Implement this to create different storage strategies:

```python
from pydb.interface import StorageEngine

class MyCustomStorage(StorageEngine):
    def set(self, key: bytes, value: bytes) -> None: ...
        # Your custom storage logic
    
    def get(self, key: bytes) -> bytes: ...
        # Your custom retrieval logic
    
    def delete(self, key: bytes) -> None: ...
        # Your custom deletion logic
```

**Use cases**: Append-only logs for write optimization, B-tree storage for balanced reads/writes, LSM-tree implementations for high write throughput, in-memory stores for maximum speed

#### **`Index` Interface**

Control how keys are indexed for fast lookups:

```python
from pydb.interface import Index

class MyCustomIndex(Index):
    def has(self, key: bytes) -> bool: ...
        # Check if key exists
    
    def set(self, key: bytes, offset: int) -> None: ...
        # Store key-to-offset mapping
    
    def get(self, key: bytes) -> int: ...
        # Retrieve offset for key
    
    def delete(self, key: bytes) -> None: ...
        # Remove key from index
```

**Use cases**: Hash tables for O(1) lookups, B-tree indexes for range queries, skip lists for probabilistic balancing, persistent indexes for durability across restarts

#### **`File` Interface**

Customize low-level file operations and storage formats:

```python
from pydb.interface import File

class MyCustomFile(File):
    def write(self, data: bytes) -> int: ...
        # Custom write implementation
    
    def read(self, size: int = -1) -> bytes: ...
        # Custom read implementation
    
    def seek(self, offset: int, whence: int = 0) -> int: ...
        # Custom seek implementation
    
    # ... implement other required methods
```

**Use cases**: Segmented files for managing large datasets, compressed storage for space efficiency, encrypted files for data security, memory-mapped I/O for performance optimization

By mixing and matching different implementations of these interfaces, you can experiment with various database architectures and understand how different design choices affect performance, durability, and scalability.

## Learning Objectives

By exploring this project, you will gain understanding of:

- Low-level data storage and file I/O operations
- Memory management and buffer pooling
- B-tree and other indexing structures
- Query optimization techniques
- ACID properties implementation
- Concurrency control mechanisms (locking, MVCC)
- Write-ahead logging and recovery protocols

## Documentation

Additional documentation can be found in the `docs/` directory, including:

- Architecture decisions and design patterns
- Implementation notes and development backlog
- Technical references and learning resources

## How to Contribute

This is an educational project and contributions are welcome! Whether you're:

- Fixing bugs or improving code quality
- Adding documentation or examples
- Suggesting new features or optimizations
- Sharing learning resources

Please feel free to open issues or submit pull requests.

## License

This project is licensed under the [MIT License](LICENSE).

## Acknowledgments

This project is inspired by educational resources on database internals and aims to provide a practical learning experience for understanding how databases work at a fundamental level.
