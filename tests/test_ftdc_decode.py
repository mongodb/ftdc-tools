from typing import Dict, List, Tuple

import pytest

from aiofile import async_open

from ftdc_tools.ftdc_decoder import FTDC


def validateFTDCRecord(record: Dict[Tuple, int]) -> None:
    required_keys = [
        (b"ts",),
        (b"id",),
        (b"counters", b"n"),
        (b"counters", b"ops"),
        (b"counters", b"size"),
        (b"counters", b"errors"),
        (b"timers", b"dur"),
        (b"timers", b"total"),
        (b"gauges", b"state"),
        (b"gauges", b"workers"),
        (b"gauges", b"failed"),
    ]
    assert len(record) == len(required_keys)
    for key in required_keys:
        assert key in record.keys()
        assert type(record[key]) == int


class TestFTDCDecode:
    def test_ftdc_rollup_non_streaming(self, test_data: List[Dict]) -> None:
        for test in test_data:
            file = open(test["ftdc_file"], "rb")
            ftdc_data = file.read()
            file.close()
            record_count = 0
            for ftdc_row in FTDC(ftdc_data):
                print(ftdc_row)
                validateFTDCRecord(ftdc_row)
                record_count += 1

            assert record_count == test["doc_count"]

    def test_ftdc_rollup_streaming(self, test_data: List[Dict]) -> None:
        for test in test_data:
            file = open(test["ftdc_file"], "rb")
            record_count = 0
            for ftdc_row in FTDC(file):
                validateFTDCRecord(ftdc_row)
                record_count += 1

            assert record_count == test["doc_count"]

    @pytest.mark.asyncio
    async def test_async_ftdc_rollup_streaming(self, test_data: List[Dict]) -> None:
        for test in test_data:
            async with async_open(test["ftdc_file"], "rb") as file:
                record_count = 0
                async for ftdc_row in FTDC(file.iter_chunked(10000)):
                    validateFTDCRecord(ftdc_row)
                    record_count += 1

            assert record_count == test["doc_count"]

    @pytest.mark.asyncio
    async def test_async_ftdc_rollup_streaming_with_memory(
        self, test_data: List[Dict]
    ) -> None:
        for test in test_data:
            async with async_open(test["ftdc_file"], "rb") as file:
                record_count = 0
                async for ftdc_row in FTDC(file.iter_chunked(1000), memory=10000):
                    validateFTDCRecord(ftdc_row)
                    record_count += 1

            assert record_count == test["doc_count"]
