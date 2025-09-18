from abc import ABC, abstractmethod


class StorageEngine(ABC):
    @abstractmethod
    def set(self):
        raise NotImplementedError

    @abstractmethod
    def get(self):
        raise NotImplementedError

    @abstractmethod
    def delete(self):
        raise NotImplementedError
