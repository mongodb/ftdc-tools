"""Package to decode FTDC data."""
import datetime
import io
import zlib

from collections import OrderedDict
from collections.abc import MutableMapping
from dataclasses import dataclass
from typing import IO, Any, BinaryIO, Callable, Iterator, List, Tuple, Union

import bson

from bson.codec_options import CodecOptions

# Values an FTDC value may take.
FTDCValue = Union[datetime.datetime, int, bool, OrderedDict[str, Any]]


# Type for a FTDC doc returned from an iter function.
# In principle a FTDCDoc should only contain FTDCValues, but
# we can't embed this definition in that one. (mypy doesn't
# support cyclic definitions.)
FTDCDoc = OrderedDict[str, Any]


# Types for an undecoded BSON doc.
BSONSingleValue = Union[datetime.datetime, bson.int64.Int64, bool]
BSONValue = Union[BSONSingleValue, MutableMapping[str, Any]]


# Path to a key in a FTDC document.
KeyPath = Tuple[str, ...]


@dataclass
class MetricChunk:
    """Dataclass representing a single chunk's metrics (one column)."""

    key_path: Tuple[str, ...]
    values: List[FTDCValue]
    delta_constructor: Callable[..., Any]


# We have mypy ignore CodecOptions and its uses throughout; it complains about
# usage recommended in the pymongo bson docs.
ftdc_bson_options = CodecOptions(document_class=OrderedDict)  # type: ignore


def decode_iter(ftdc: bytes) -> Iterator[FTDCDoc]:
    """Iterate over all FTDC docs in a set of bytes."""
    for chunk in bson.decode_iter(ftdc, codec_options=ftdc_bson_options):  # type: ignore
        for doc in _iter_chunk(chunk):  # type: ignore
            yield doc


def decode_file_iter(file_obj: Union[BinaryIO, IO]) -> Iterator[FTDCDoc]:
    """Iterate over all FTDC docs in a stream."""
    for chunk in bson.decode_file_iter(file_obj, codec_options=ftdc_bson_options):  # type: ignore
        for doc in _iter_chunk(chunk):  # type: ignore
            yield doc


def _iter_chunk(chunk: FTDCDoc) -> Iterator[FTDCDoc]:
    """Iterate over a single FTDC chunk."""
    raw_data = chunk["data"]

    # Remove 4 bytes to drop header, decompressing the rest.
    bson_data = io.BytesIO(zlib.decompress(raw_data[4:]))

    # The metrics chunk, which is *not* bson, first
    # contains a bson document which begins the sample.
    # We retrieve just that doc. The rest cannot be parsed as bson.
    ref_doc: FTDCDoc = bson.decode_file_iter(
        bson_data, codec_options=ftdc_bson_options  # type: ignore
    ).__next__()
    metrics = _get_metrics_from_ref_doc(ref_doc)

    # The next four bytes tell us how many metrics (fields) to expect per event in this chunk.
    # If that doesn't match the reference doc, error out.
    metric_count = int.from_bytes(bson_data.read(4), byteorder="little", signed=False)
    if metric_count != len(metrics):
        raise ValueError(
            "Metrics mismatch, file likely corrupted."
            f" Expected {metric_count} metrics but found {len(ref_doc)}"
        )

    # The next four bytes tell how many delta-encoded events to expect for each series
    # in this chunk.
    delta_count = int.from_bytes(bson_data.read(4), byteorder="little", signed=False)

    # The rest of the chunk is a bunch of varint-encoded deltas for each metric in order.
    zero_count = 0
    for metric in metrics:
        # The first value is pre-populated from the reference doc.
        previous_val = metric.values[0]
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
        raise ValueError(
            "Metrics chunk contains extra deltas. Expected only {delta_count}"
        )

    # This is the main iteration / generator.
    for i in range(len(metrics[0].values)):
        doc = FTDCDoc()
        for metric in metrics:
            # Construct a doc for this index across all metrics slices.
            cur = doc
            for j in range(len(metric.key_path) - 1):
                key = metric.key_path[j]
                if key not in cur:
                    cur[key] = FTDCDoc()
                cur = cur[key]
            cur[metric.key_path[-1]] = metric.values[i]
        yield doc


def _get_metrics_from_ref_doc(ref_doc: FTDCDoc) -> List[MetricChunk]:
    """Return a list of MetricChunks that are deciphered from the reference doc."""
    metrics = []
    for key_path, value in _get_paths_and_vals(ref_doc):
        bson_translate, delta_constructor = _get_constructors_for_val(value)
        metrics.append(
            MetricChunk(
                key_path=key_path,
                values=[bson_translate(value)],
                delta_constructor=delta_constructor,
            )
        )
    return metrics


def _get_paths_and_vals(
    d: MutableMapping[Any, BSONValue], key_path: List[str] = []
) -> List[Tuple[KeyPath, BSONSingleValue]]:
    """Return the paths to all leaves and the leaf-level values themselves."""
    output = []
    for key, value in d.items():
        key_path.append(key)
        if isinstance(value, MutableMapping):
            output += _get_paths_and_vals(value, key_path)
        else:
            output.append((tuple(key_path), value))
        key_path.pop()
    return output


def _get_constructors_for_val(
    val: BSONValue,
) -> Tuple[Callable[..., Any], Callable[..., Any]]:
    """
    Return two functions.

    One for converting the initial bson value to a pythonic value.

    Another for converting integer delta values to that type's delta.
    """

    def identity(x: Any) -> Any:
        return x

    def int_to_timedelta(x: int) -> datetime.timedelta:
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


def _decode_varint(data_stream: Union[BinaryIO, IO]) -> int:
    """
    Use magic to decode a varint.

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
