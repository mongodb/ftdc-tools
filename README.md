# ftdc-tools - Pure Python package for FTDC
This package provides tools required to decode and process FTDC data. So whats unique about this package?

* This a pure python package that relies on pymongo for decoding FTDC data.
* This package provides streaming support. With this feature the whole FTDC files is not loaded in memory and is capable of processing large FTDC files.

## Getting Started - Users
```
pip install ftdc-tools
```

## Usage

### Warning
This package is still in beta. In particular, all bson values are not guaranteed to be supported. This package is used to process Genny/Poplar client-side FTDC files and hasn't been tested with mongo server FTDC.

### Decoding FTDC file from URL - Streaming approach
```
from ftdc_tools.decode import decode_file_iter
import requests
url = "https://genny-metrics.s3.amazonaws.com/performance_linux_wt_repl_genny_scale_InsertRemove_patch_b2098c676bdc64e3194734fa632b133c47496646_61f955933066150fca890e4a_22_02_01_15_58_36_0/canary_InsertRemove.ActorFinished"

# Streaming FTDC data
response = requests.get(url, stream=True)
for ftdc_doc in decode_file_iter(response.raw):
    print(ftdc_doc)
```

### Decoding FTDC file from URL - Non-streaming approach
```
from ftdc_tools.decode import decode_iter
import requests
url = "https://genny-metrics.s3.amazonaws.com/performance_linux_wt_repl_genny_scale_InsertRemove_patch_b2098c676bdc64e3194734fa632b133c47496646_61f955933066150fca890e4a_22_02_01_15_58_36_0/canary_InsertRemove.ActorFinished"

response = requests.get(url)
for ftdc_doc in decode_iter(response.content):
    print(ftdc_doc)
```

### Rolling up a FTDC file 

Rolling up a FTDC file creates a set of summary statistics based on a certain expected schema.
The ClientPerformanceStatistics rollup expects the Genny/Poplar schema described [here](https://github.com/10gen/performance-tooling-docs/blob/main/getting_started/intrarun_data_generation.md#client-side-intra-run-data).

```
from ftdc_tools.decode import decode_file_iter
from ftdc_tools.rollup.client_perf import ClientPerformanceStatistics
import requests
url = "https://genny-metrics.s3.amazonaws.com/performance_linux_wt_repl_genny_scale_InsertRemove_patch_b2098c676bdc64e3194734fa632b133c47496646_61f955933066150fca890e4a_22_02_01_15_58_36_0/canary_InsertRemove.ActorFinished"

# Streaming FTDC data
response = requests.get(url, stream=True)
roll = ClientPerformanceStatistics()
for ftdc_doc in decode_file_iter(response.raw):
    roll.add_doc(ftdc_doc)
print("Statistics: ", roll.all_statistics)
```


## Getting Started - Developers

Getting the code:
```
$ git clone git@github.com:mongodb/ftdc-tools.git
$ cd ftdc-tools
```

Installation

```
$ pip install poetry
$ poetry install
```

Testing/linting:
```
$ poetry run pytest
```

### Formatting/Lint Fixing
```
poetry run black src tests
poetry run isort src tests
poetry run autoflake --in-place --remove-all-unused-imports -r src tests
```

### Committing
In your PR, you MUST bump the semver package version in pyproject.toml.
