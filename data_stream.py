import abc
import typing


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self._data: list[tuple[int, str]] = []
        self._rank: int = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._data:
            raise IndexError("No data available")
        item = self._data[0]
        self._data = self._data[1:]
        return item

    def total_processed(self) -> int:
        return self._rank

    def remaining(self) -> int:
        return len(self._data)

    def processor_name(self) -> str:
        return type(self).__name__.replace("Processor", " Processor")


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, bool):
            return False
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(
                isinstance(x, (int, float)) and not isinstance(x, bool)
                for x in data
            )
        return False

    def ingest(
        self,
        data: typing.Union[int, float, list[typing.Union[int, float]]]
    ) -> None:
        if not self.validate(data):
            raise TypeError("Improper numeric data")
        if isinstance(data, list):
            for item in data:
                self._data.append((self._rank, str(item)))
                self._rank += 1
        else:
            self._data.append((self._rank, str(data)))
            self._rank += 1


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(x, str) for x in data)
        return False

    def ingest(
        self,
        data: typing.Union[str, list[str]]
    ) -> None:
        if not self.validate(data):
            raise TypeError("Improper text data")
        if isinstance(data, list):
            for item in data:
                self._data.append((self._rank, item))
                self._rank += 1
        else:
            self._data.append((self._rank, data))
            self._rank += 1


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict):
            return (
                isinstance(data.get("log_level"), str)
                and isinstance(data.get("log_message"), str)
            )
        if isinstance(data, list):
            return all(
                isinstance(x, dict)
                and isinstance(x.get("log_level"), str)
                and isinstance(x.get("log_message"), str)
                for x in data
            )
        return False

    def ingest(
        self,
        data: typing.Union[dict[str, str], list[dict[str, str]]]
    ) -> None:
        if not self.validate(data):
            raise TypeError("Improper log data")
        if isinstance(data, list):
            for item in data:
                combined = f"{item['log_level']}: {item['log_message']}"
                self._data.append((self._rank, combined))
                self._rank += 1
        else:
            combined = f"{data['log_level']}: {data['log_message']}"
            self._data.append((self._rank, combined))
            self._rank += 1


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []
        print("Initialize Data Stream...")

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for element in stream:
            handled = False
            for proc in self._processors:
                if proc.validate(element):
                    proc.ingest(element)
                    handled = True
                    break
            if not handled:
                print(
                    f"DataStream error - Can't process element in stream:"
                    f"  {element}"
                )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for proc in self._processors:
            print(
                f"{proc.processor_name()}:  total {proc.total_processed()}"
                f" items processed, remaining {proc.remaining()}"
                f" on processor"
            )


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    print()

    stream = DataStream()
    stream.print_processors_stats()
    print()

    print("Registering Numeric Processor")
    numeric = NumericProcessor()
    stream.register_processor(numeric)

    batch1: list[typing.Any] = [
        'Hello world',
        [3.14, -1, 2.71],
        [
            {'log_level': 'WARNING',
             'log_message': 'Telnet access! Use ssh instead'},
            {'log_level': 'INFO',
             'log_message': 'User wil is connected'}
        ],
        42,
        ['Hi', 'five']
    ]
    print(f"Send first batch of data on stream:  {batch1}")
    stream.process_stream(batch1)
    stream.print_processors_stats()
    print()

    print("Registering other data processors")
    text = TextProcessor()
    log = LogProcessor()
    stream.register_processor(text)
    stream.register_processor(log)

    print("Send the same batch again")
    stream.process_stream(batch1)
    stream.print_processors_stats()
    print()

    print(
        "Consume some elements from the data processors:"
        "  Numeric 3, Text 2, Log 1"
    )
    for _ in range(3):
        numeric.output()
    for _ in range(2):
        text.output()
    for _ in range(1):
        log.output()
    stream.print_processors_stats()
