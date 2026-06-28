
from abc import ABC
from abc import abstractmethod
from typing import Any
from data_stream import LogProcessor


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data = None
        self._is_valid = False
        self.store_data: list[str] = []

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.store_data:
            return (0, "")
        return (0, self.store_data.pop(0))


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        self.store_data: list[str] = []

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(isinstance(i, (int, float)) for i in data)
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        items = [data] if isinstance(data, (int, float)) else data
        self.store_data.extend(map(str, items))


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        self.store_data: list[str] = []

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(i, str) for i in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        items = data if isinstance(data, list) else [data]
        self.store_data.extend(items)


class LogicProcessor(DataProcessor):
    def __init__(self) -> None:
        self.store_data: list[str] = []

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return 'log_level' in data and 'log_message' in data
        if isinstance(data, list):
            return all(self.validate(i) for i in data)
        return False

    def ingest(self, data: dict[str, Any]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        items = [data] if isinstance(data, dict) else data
        for log in items:
            self.store_data.append(f"{log['log_level']}: {log['log_message']}")


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===\n")
    print("Testing Numeric Processor...")
    number = NumericProcessor()
    print(f"Trying to validate input '42': {number.validate(42)}")
    print(f"Trying to validate input 'Hello': {number.validate('Hello')}")
    try:
        print("Test invalid ingestion of string 'foo' without prior "
              "validation:")
        number.ingest('foo')
    except ValueError as e:
        print(f"Got exception: {e}")
    number.ingest([1, 2, 3, 4, 5])
    print("Processing data: [1, 2, 3, 4, 5]")
    print("Extracting 3 values...")
    for i in range(3):
        print(f"Numeric value {i}: {number.output()[1]}")
    print("\nTesting Text Processor...")
    text = TextProcessor()
    print(f"Trying to validate input '42': {text.validate(42)}")
    text.ingest(['Hello', 'Nexus', 'World'])
    print("Processing data: ['Hello', 'Nexus', 'World']")
    print("Extracting 1 value...")
    print(f"Text value 0: {text.output()[1]}")
    print("\nTesting Log Processor...")
    log = LogProcessor()
    print(f"Trying to validate input 'Hello': {log.validate('Hello')}")
    logs = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    ]
    log.ingest(logs)
    print(f"Processing data: {logs}")
    print("Extracting 2 values...")
    for i in range(2):
        print(f"Log entry {i}: {log.output()[1]}")
