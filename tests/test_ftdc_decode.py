import json

from datetime import timezone
from typing import Dict, List

from src.ftdc_tools.decode import decode_file_iter

# import pytest

# from aiofile import async_open


class TestFTDCDecode:
    def test_ftdc_rollup_file_iter(self, test_data: List[Dict]) -> None:
        for test in test_data:
            with open(test["ftdc_file"], "rb") as f, open(test["json_file"], "r") as j:
                index = 0
                print("json file: ", test["json_file"])
                for ftdc_doc in decode_file_iter(f):

                    json_doc = json.loads(j.readline())
                    # This converts nested OrderedDicts to dicts.
                    # We can't just load the json file as an OrderedDict because apparently
                    # curator exports fields in a different order than pymongo. (I'd defer to pymongo.)
                    print(ftdc_doc["ts"].timestamp())
                    ftdc_doc["ts"] = int(
                        ftdc_doc["ts"].replace(tzinfo=timezone.utc).timestamp() * 1000
                    )  # - 18000000  # Microsecond conversion
                    ftdc_doc = json.loads(json.dumps(ftdc_doc))
                    assert ftdc_doc == json_doc
                    index += 1
                assert not j.read(1)

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
