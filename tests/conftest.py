from typing import Dict, List

import pytest


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
