import pytest
from typing import List, Dict


@pytest.fixture
def test_data() -> List[Dict[str, object]]:
    return [
        {
            "ftdc_file": "./tests/test_bson_files/test_data_ftdc_file_0_doc",
            "doc_count": 1,
        },
        {
            "ftdc_file": "./tests/test_bson_files/test_data_ftdc_file_1_doc",
            "doc_count": 1,
        },
        {
            "ftdc_file": "./tests/test_bson_files/test_data_ftdc_file_101_doc",
            "doc_count": 101,
        },
        {
            "ftdc_file": "./tests/test_bson_files/test_data_ftdc_file_1102029_doc",
            "doc_count": 1102029,
        },
    ]
