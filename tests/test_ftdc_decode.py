import json

from datetime import timezone
from typing import Any, Dict, List

from src.ftdc_tools.decode import decode_file_iter, decode_iter


def test_ftdc_decode_file_iter(test_data: List[Dict]) -> None:
    for test in test_data:
        with open(test["ftdc_file"], "rb") as f, open(test["json_file"], "r") as j:
            index = 0
            for ftdc_doc in decode_file_iter(f):
                json_doc = json.loads(j.readline())
                ftdc_doc = ftdc_to_dict(ftdc_doc)
                assert ftdc_doc == json_doc
                index += 1
            assert not j.read(1)  # Assert no more json.


def test_ftdc_decode_iter(test_data: List[Dict]) -> None:
    for test in test_data:
        with open(test["ftdc_file"], "rb") as f:
            whole_ftdc_file = f.read()
        with open(test["json_file"], "r") as j:
            index = 0
            for ftdc_doc in decode_iter(whole_ftdc_file):
                json_doc = json.loads(j.readline())
                ftdc_doc = ftdc_to_dict(ftdc_doc)
                assert ftdc_doc == json_doc
                index += 1
            assert not j.read(1)  # Assert no more json.


def ftdc_to_dict(ftdc_doc) -> Dict[str, Any]:
    # This converts nested OrderedDicts to dicts.
    # We can't just load the json file as an OrderedDict because apparently
    # curator exports fields in a different order than pymongo. (I'd defer to pymongo.)
    ftdc_doc["ts"] = int(ftdc_doc["ts"].replace(tzinfo=timezone.utc).timestamp() * 1000)
    return json.loads(json.dumps(ftdc_doc))
