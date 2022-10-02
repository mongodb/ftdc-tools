"""Package to decode FTDC data."""
import collections
import bson
import io
import zlib
import datetime
import time
from bson.codec_options import CodecOptions

from typing import Any, Callable, List, Tuple
from collections.abc import MutableMapping
from dataclasses import dataclass


# TODO: Generic?
@dataclass
class MetricChunk:
    key_path: Any
    values: List[Any]
    reference_val: Any
    delta_constructor: Callable[Any, Any]


ftdc_bson_options = CodecOptions(document_class=collections.OrderedDict)


def decode_iter(ftdc):
    """Iterate over all docs in a FTDC stream."""
    for chunk in bson.decode_iter(ftdc, codec_options=ftdc_bson_options):
        for doc in _iter_chunk(chunk):
            yield doc


def decode_file_iter(ftdc):
    """Iterate over all docs in a FTDC stream."""
    for chunk in bson.decode_file_iter(ftdc, codec_options=ftdc_bson_options):
        for doc in _iter_chunk(chunk):
            yield doc
    
                    
def _iter_chunk(chunk):
    """Iterate over a single FTDC chunk."""
    raw_data = chunk["data"]

    # Remove 4 bytes to drop header, decompressing the rest.
    bson_data = io.BytesIO(zlib.decompress(raw_data[4:]))

    # The metrics chunk, which is *not* bson, first
    # contains a bson document which begins the sample.
    # We retrieve just that doc. The rest cannot be parsed as bson.
    # TODO: Ordered dict?
    ref_doc = bson.decode_file_iter(bson_data, codec_options=ftdc_bson_options).__next__()
    metrics = _get_metrics_from_ref_doc(ref_doc)

    # The next four bytes tell us how many metrics (fields) to expect per event in this chunk.
    # If that doesn't match the reference doc, error out.
    metric_count = int.from_bytes(bson_data.read(4), byteorder="little", signed=False)
    if metric_count != len(metrics):
        raise ValueError("Metrics mismatch, file likely corrupted."
                            f" Expected {metric_count} metrics but found {len(ref_doc)}")

    # The next four bytes tell how many delta-encoded events to expect for each series
    # in this chunk.
    delta_count = int.from_bytes(bson_data.read(4), byteorder="little", signed=False)

    # The rest of the chunk is a bunch of varint-encoded deltas for each metric in order.
    zero_count = 0
    for metric in metrics:
        previous_val = metric.reference_val
        for _ in range(delta_count):
            if zero_count != 0:
                delta = 0
                zero_count -= 1
            else:
                delta = _decode_varint(bson_data)
                if delta == 0:
                    zero_count = _decode_varint(bson_data)
            cur_val = previous_val + metric.delta_constructor(delta)
            metric.values.append(cur_val)
            previous_val = cur_val

    if bson_data.read():
        raise ValueError("Metrics chunk contains extra deltas. Expected only {delta_count}")

    yield ref_doc

    for i in range(delta_count):
        doc = {}
        for metric in metrics:
            cur = doc
            for j in range(len(metric.key_path)-1):
                key = metric.key_path[j]
                if key not in cur:
                    cur[key] = {}
                cur = cur[key]
            cur[metric.key_path[-1]] = metric.values[i]
        yield doc


def _decode_varint(data_stream):
    """
    Magically decode a varint.

    More notes about varints: https://developers.google.com/protocol-buffers/docs/encoding
    """
    res = 0
    shift = 0
    while True:
        b = int.from_bytes(data_stream.read(1), byteorder="little")
        res |= (b & 0x7F) << shift
        if not (b & 0x80):
            if res > 0x7FFFFFFFFFFFFFFF:  # negative 64-bit value
                res = int(res - 0x10000000000000000)
            return res
        shift += 7


def _get_metrics_from_ref_doc(ref_doc):
    metrics = []
    key_path = []
    for key_path, value in _get_paths_and_vals(ref_doc):
        bson_translate, delta_constructor = _get_constructors_for_val(value)
        metrics.append(MetricChunk(key_path=key_path, values=[],
                              reference_val=bson_translate(value),
                              delta_constructor=delta_constructor))
    return metrics

def _get_paths_and_vals(d, key_path=[]):
    output = []
    for key, value in d.items():
        key_path.append(key)
        if isinstance(value, MutableMapping):
            output += _get_paths_and_vals(value, key_path)
        else:
            output.append((tuple(key_path), value))
        key_path.pop()
    return output


def _get_constructors_for_val(val):
    """
    Return two functions:

    One for converting the initial bson value to a pythonic value.

    Another for converting integer delta values to that type's delta.
    """

    def identity(x):
        return x

    def int_to_timedelta(x):
        return datetime.timedelta(milliseconds=x)

    # TODO: There are more possible bson, and therefore likely possible FTDC types.
    # These are just the ones that Genny & Poplar metrics outputs use. We will want
    # to consider expanding this in the future.
    if isinstance(val, datetime.datetime):
        return identity, int_to_timedelta
    elif isinstance(val, bson.int64.Int64):
        return int, identity
    elif isinstance(val, bool):
        return identity, bool
    else:
        raise ValueError(f"Unknown type found in FTDC chunk reference doc: {type(val)}")
    

def _get_metrics(d, parent_keys):
    items = []
    for key, value in to_flatten.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(_flatten_dict(value, new_key, sep=sep).items())
        else:
            items.append((new_key, value))
    return dict(items)



# Note for somewhere: BSON only has millisecond-granularity for timestamps. So even though
# genny reports TS using milliseconds, that seems to be lost when dumped to disk.
