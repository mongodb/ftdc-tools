from typing import Dict, List

from src.ftdc_tools.decode import decode_file_iter

# import pytest

# from aiofile import async_open


def validate_ftdc_record(record) -> None:
    """Do a very basic validation of a FTDC record."""
    # required_keys = [
    #    ("ts",),
    #    ("id",),
    #    ("counters", "n"),
    #    ("counters", "ops"),
    #    ("counters", "size"),
    #    ("counters", "errors"),
    #    ("timers", "dur"),
    #    ("timers", "total"),
    #    ("gauges", "state"),
    #    ("gauges", "workers"),
    #    ("gauges", "failed"),
    # ]
    # assert len(record) == len(required_keys)
    # for key in required_keys:
    #    assert key in record.keys()
    #    assert type(record[key]) == int
    top_level_keys = ["ts", "id", "counters", "timers", "gauges"]
    for index, key in enumerate(record.keys()):
        assert key == top_level_keys[index]


class TestFTDCDecode:
    def test_ftdc_rollup_file_iter(self, test_data: List[Dict]) -> None:
        for test in test_data:
            with open(test["ftdc_file"], "rb") as f:
                record_count = 0
                for ftdc_row in decode_file_iter(f):
                    validate_ftdc_record(ftdc_row)
                    record_count += 1
            assert record_count == test["doc_count"]

    # def test_ftdc_rollup_streaming(self, test_data: List[Dict]) -> None:
    #    for test in test_data:
    #        file = open(test["ftdc_file"], "rb")
    #        record_count = 0
    #        for ftdc_row in FTDC(file):
    #            validateFTDCRecord(ftdc_row)
    #            record_count += 1

    #        assert record_count == test["doc_count"]

    # @pytest.mark.asyncio
    # async def test_async_ftdc_rollup_streaming(self, test_data: List[Dict]) -> None:
    #    for test in test_data:
    #        async with async_open(test["ftdc_file"], "rb") as file:
    #            record_count = 0
    #            async for ftdc_row in FTDC(file.iter_chunked(10000)):
    #                validateFTDCRecord(ftdc_row)
    #                record_count += 1

    #        assert record_count == test["doc_count"]

    # @pytest.mark.asyncio
    # async def test_async_ftdc_rollup_streaming_with_memory(
    #    self, test_data: List[Dict]
    # ) -> None:
    #    for test in test_data:
    #        async with async_open(test["ftdc_file"], "rb") as file:
    #            record_count = 0
    #            async for ftdc_row in FTDC(file.iter_chunked(1000), memory=10000):
    #                validateFTDCRecord(ftdc_row)
    #                record_count += 1

    #        assert record_count == test["doc_count"]
