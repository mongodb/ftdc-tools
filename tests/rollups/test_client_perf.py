import json

from datetime import timezone
from typing import Any, Dict, List

import bson

from src.ftdc_tools.decode import (
    MetricChunk,
    _get_metrics_from_ref_doc,
    decode_file_iter,
    decode_iter,
)


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


def test_get_metrics_from_ref_doc() -> None:
    ref_doc = {
        "metricA": bson.int64.Int64(5),
        "metricB": bson.int64.Int64(6),
        "nestedMetric": {
            "metricC": bson.int64.Int64(7),
            "subnestedMetric": {"metricD": True},
        },
    }

    expected = [
        MetricChunk(key_path=("metricA",), values=[5], delta_constructor=lambda x: x),
        MetricChunk(key_path=("metricB",), values=[6], delta_constructor=lambda x: x),
        MetricChunk(
            key_path=("nestedMetric", "metricC"),
            values=[7],
            delta_constructor=lambda x: x,
        ),
        MetricChunk(
            key_path=("nestedMetric", "subnestedMetric", "metricD"),
            values=[True],
            delta_constructor=lambda x: x,
        ),
    ]
    actual = _get_metrics_from_ref_doc(ref_doc)
    # Can't put the identity nested function in the function under test into the expected list.
    assert len(expected) == len(actual)
    for i, expected_chunk in enumerate(expected):
        assert expected_chunk.key_path == actual[i].key_path
        assert expected_chunk.values == actual[i].values
        assert expected_chunk.delta_constructor(
            756
        ) == expected_chunk.delta_constructor(756)


def test_get_metrics_from_empty_ref_doc() -> None:
    assert _get_metrics_from_ref_doc({}) == []


def ftdc_to_dict(ftdc_doc) -> Dict[str, Any]:
    # This converts nested OrderedDicts to dicts.
    # We can't just load the json file as an OrderedDict because apparently
    # curator exports fields in a different order than pymongo. (I'd defer to pymongo.)
    ftdc_doc["ts"] = int(ftdc_doc["ts"].replace(tzinfo=timezone.utc).timestamp() * 1000)
    return json.loads(json.dumps(ftdc_doc))
