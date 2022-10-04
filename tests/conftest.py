from datetime import datetime
from typing import Dict, List

import pytest

from ftdc_tools.decode import FTDCDoc


@pytest.fixture
def test_data() -> List[Dict[str, object]]:
    return [
        {
            "ftdc_file": "./tests/test_files/ftdc/test_data_ftdc_file_empty_doc",
            "json_file": "./tests/test_files/json/test_data_ftdc_file_empty_doc",
            "doc_count": 0,
        },
        {
            "ftdc_file": "./tests/test_files/ftdc/test_data_ftdc_file_0_doc",
            "json_file": "./tests/test_files/json/test_data_ftdc_file_0_doc",
            "doc_count": 1,
        },
        {
            "ftdc_file": "./tests/test_files/ftdc/test_data_ftdc_file_1_doc",
            "json_file": "./tests/test_files/json/test_data_ftdc_file_1_doc",
            "doc_count": 1,
        },
        {
            "ftdc_file": "./tests/test_files/ftdc/test_data_ftdc_file_101_doc",
            "json_file": "./tests/test_files/json/test_data_ftdc_file_101_doc",
            "doc_count": 101,
        },
    ]


@pytest.fixture
def mock_ftdc_stream_output() -> List[FTDCDoc]:
    return [
        {
            "ts": datetime.fromtimestamp(1643735930.767),
            "id": 0,
            "counters": {
                "n": 1,
                "ops": 1,
                "size": 0,
                "errors": 0,
            },
            "timers": {"dur": 366, "total": 180009841025},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.768),
            "id": 1,
            "counters": {"n": 2, "ops": 2, "size": 10, "errors": 4},
            "timers": {"dur": 722, "total": 180009845702},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.769),
            "id": 2,
            "counters": {"n": 3, "ops": 3, "size": 10, "errors": 0},
            "timers": {"dur": 1603, "total": 180009848.249},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.770),
            "id": 3,
            "counters": {"n": 4, "ops": 4, "size": 10, "errors": 0},
            "timers": {"dur": 1999, "total": 180009926035},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.771),
            "id": 4,
            "counters": {"n": 5, "ops": 5, "size": 15, "errors": 0},
            "timers": {"dur": 2387, "total": 180009928.537},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.772),
            "id": 5,
            "counters": {"n": 6, "ops": 6, "size": 15, "errors": 0},
            "timers": {"dur": 2785, "total": 180009953.282},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.773),
            "id": 6,
            "counters": {"n": 7, "ops": 7, "size": 15, "errors": 1},
            "timers": {"dur": 3133, "total": 180009961.887},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.774),
            "id": 7,
            "counters": {"n": 8, "ops": 8, "size": 16, "errors": 0},
            "timers": {"dur": 3555, "total": 180009966538},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.775),
            "id": 8,
            "counters": {"n": 9, "ops": 9, "size": 17, "errors": 0},
            "timers": {"dur": 3854, "total": 180009976167},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.776),
            "id": 9,
            "counters": {"n": 10, "ops": 10, "size": 18, "errors": 0},
            "timers": {"dur": 4156, "total": 180009978028},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
    ]


@pytest.fixture
def mock_non_linear_ftdc_stream_output() -> list:
    """
    In the below output the third row has the earliest timestamp and will be considered
    the first row
    +-----------------------+-----------------+---------------------+-----------------------+
    | ts                    | timers_duration | actual_duration(ms) | start_ts              |
    +-----------------------+-----------------+---------------------+-----------------------+
    | 1643735930767.0000000 |          366111 |            0.000366 | 1643735930767.0000000 |
    +-----------------------+-----------------+---------------------+-----------------------+
    | 1643735930768.0000000 |         7221111 |               6.855 | 1643735930761.1500000 |
    +-----------------------+-----------------+---------------------+-----------------------+
    | 1643735930769.0000000 |        16031111 |                8.81 | 1643735930760.1900000 |
    +-----------------------+-----------------+---------------------+-----------------------+
    """
    return [
        {
            "ts": datetime.fromtimestamp(1643735930.767),
            "id": 0,
            "counters": {
                "n": 1,
                "ops": 1,
                "size": 0,
                "errors": 0,
            },
            "timers": {"dur": 366111, "total": 180009841025},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.768),
            "id": 1,
            "counters": {
                "n": 2,
                "ops": 2,
                "size": 10,
                "errors": 4,
            },
            "timers": {"dur": 7221111, "total": 180009845.702},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.769),
            "id": 2,
            "counters": {
                "n": 3,
                "ops": 3,
                "size": 10,
                "errors": 0,
            },
            "timers": {"dur": 16031111, "total": 180009848249},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.770),
            "id": 3,
            "counters": {
                "n": 4,
                "ops": 4,
                "size": 10,
                "errors": 0,
            },
            "timers": {"dur": 19991111, "total": 180009926035},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.771),
            "id": 4,
            "counters": {
                "n": 5,
                "ops": 5,
                "size": 15,
                "errors": 0,
            },
            "timers": {"dur": 23871111, "total": 180009928537},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.772),
            "id": 5,
            "counters": {
                "n": 6,
                "ops": 6,
                "size": 15,
                "errors": 0,
            },
            "timers": {"dur": 27851111, "total": 180009953282},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.773),
            "id": 6,
            "counters": {
                "n": 7,
                "ops": 7,
                "size": 15,
                "errors": 1,
            },
            "timers": {"dur": 31331111, "total": 180009961887},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.774),
            "id": 7,
            "counters": {
                "n": 8,
                "ops": 8,
                "size": 16,
                "errors": 0,
            },
            "timers": {"dur": 35551111, "total": 180009966538},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.775),
            "id": 8,
            "counters": {
                "n": 9,
                "ops": 9,
                "size": 17,
                "errors": 0,
            },
            "timers": {"dur": 38541111, "total": 180009976167},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
        {
            "ts": datetime.fromtimestamp(1643735930.776),
            "id": 9,
            "counters": {
                "n": 10,
                "ops": 10,
                "size": 18,
                "errors": 0,
            },
            "timers": {"dur": 41561111, "total": 180009978028},
            "gauges": {"state": 0, "workers": 1, "failed": False},
        },
    ]
