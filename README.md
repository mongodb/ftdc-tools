# ftdc-tools - Pure Python package for FTDC
This package provides tools required to decode and process FTDC data. So whats unique about this package

* This a pure python package and does not rely on any external binaries for decoding FTDC data.
* This package provides streaming support. With this feature the whole FTDC files is not loaded in memory and is capable of processing large FTDC file.
* Async support for faster processing.

## Getting Started - Users
```
pip install ftdc-tools
```

## Usage

### Decoding FTDC file from URL - Streaming approach
```
from ftdc_tools.ftdc_decoder import FTDC
import requests
url = "https://genny-metrics.s3.amazonaws.com/performance_linux_wt_repl_genny_scale_InsertRemove_patch_b2098c676bdc64e3194734fa632b133c47496646_61f955933066150fca890e4a_22_02_01_15_58_36_0/canary_InsertRemove.ActorFinished"

# Streaming FTDC data
response = requests.get(url, stream=True)
for ftdc_row in FTDC(response.raw):
    print(ftdc_row)
```

### Decoding FTDC file from URL - Non-streaming approach
```
from ftdc_tools.ftdc_decoder import FTDC
import requests
url = "https://genny-metrics.s3.amazonaws.com/performance_linux_wt_repl_genny_scale_InsertRemove_patch_b2098c676bdc64e3194734fa632b133c47496646_61f955933066150fca890e4a_22_02_01_15_58_36_0/canary_InsertRemove.ActorFinished"

response = requests.get(url)
for ftdc_row in FTDC(response.content):
    print(ftdc_row)
```

### Decoding FTDC file from URL - Aysnc streaming approach
```
import asyncio
import aiohttp
from ftdc_tools.ftdc_decoder import FTDC
url = "https://genny-metrics.s3.amazonaws.com/performance_linux_wt_repl_genny_scale_InsertRemove_patch_b2098c676bdc64e3194734fa632b133c47496646_61f955933066150fca890e4a_22_02_01_15_58_36_0/canary_InsertRemove.ActorFinished"

async def decode_ftdc():
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            async for x in FTDC(resp.content.iter_chunked(10000)):
                print(x)


asyncio.run(decode_ftdc())
```

### Decoding FTDC file from URL - Using all the available memory.
One of the disadvantages of using a streaming approach is that it's slow compared to in-memory processing.
To optimize memory usage and achieve the best performance for available memory, users can pass the memory option to the FTDC class.
When combined with an async streaming approach, users should achieve the best possible performance with available memory.
```
import asyncio
import aiohttp
from ftdc_tools.ftdc_decoder import FTDC
url = "https://genny-metrics.s3.amazonaws.com/performance_linux_wt_repl_genny_scale_InsertRemove_patch_b2098c676bdc64e3194734fa632b133c47496646_61f955933066150fca890e4a_22_02_01_15_58_36_0/canary_InsertRemove.ActorFinished"

async def decode_ftdc():
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            async for x in FTDC(resp.content.iter_chunked(10000), memory=10000*1000):
                print(x)


asyncio.run(decode_ftdc())
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
$ poetry run black ftdc_tools tests
$ poetry run isort ftdc_tools tests
$ poetry run pytest
$ poetry run flake8
```
