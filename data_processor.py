from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Dict, Union


class DataProcessor(ABC):
    data_store: List[str]

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> Tuple[int, str]:
        if not self.data_store:
            return (0, "")
        return (0, self.data_store.pop(0))


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        self.data_store: List[str] = []

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(isinstance(i, (int, float)) for i in data)
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        items: List[Union[int, float]] = data if isinstance(data, list) \
            else [data]
        self.data_store.extend([str(i) for i in items])


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        self.data_store: List[str] = []

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(i, str) for i in data)
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        items: List[str] = data if isinstance(data, list) else [data]
        self.data_store.extend(items)


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        self.data_store: List[str] = []

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return 'log_level' in data and 'log_message' in data
        if isinstance(data, list):
            return all(self.validate(i) for i in data)
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        items: List[Dict[str, str]] = data if isinstance(data, list) \
            else [data]
        for i in items:
            self.data_store.append(f"{i['log_level']}: {i['log_message']}")


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")
    print("Testing Numeric Processor...")
    np = NumericProcessor()
    print(f"Trying to validate input '42': {np.validate(42)}")
    print(f"Trying to validate input 'Hello': {np.validate('Hello')}")
    try:
        print("Test invalid ingestion of string 'foo' without prior "
              "validation:")
        np.ingest('foo')
    except ValueError as e:
        print(f"Got exception: {e}")
    np.ingest([1, 2, 3, 4, 5])
    print("Processing data: [1, 2, 3, 4, 5]")
    print("Extracting 3 values...")
    for i in range(3):
        print(f"Numeric value {i}: {np.output()[1]}")

    print("Testing Text Processor...")
    tp = TextProcessor()
    print(f"Trying to validate input '42': {tp.validate(42)}")
    tp.ingest(['Hello', 'Nexus', 'World'])
    print("Processing data: ['Hello', 'Nexus', 'World']")
    print("Extracting 1 value...")
    print(f"Text value 0: {tp.output()[1]}")

    print("Testing Log Processor...")
    lp = LogProcessor()
    print(f"Trying to validate input 'Hello': {lp.validate('Hello')}")
    logs = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    ]
    lp.ingest(logs)
    print(f"Processing data: {logs}")
    print("Extracting 2 values...")
    for i in range(2):
        print(f"Log entry {i}: {lp.output()[1]}")
