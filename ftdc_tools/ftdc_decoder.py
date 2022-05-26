"""Package to decode FTDC data."""
import collections
import struct
import zlib

from typing import (
    AsyncGenerator,
    AsyncIterator,
    ByteString,
    Dict,
    Generator,
    Iterator,
    Tuple,
    Union,
)

_int32 = struct.Struct("<i")
_uint32 = struct.Struct("<I")
_int64 = struct.Struct("<q")
_uint64 = struct.Struct("<Q")
_double = struct.Struct("<d")


class Chunk(collections.OrderedDict):
    """Defines a chuck of BSON data."""

    bson_len: int
    chunk_len: int
    nsamples: int


class FTDC(collections.abc.Iterable):
    """
    Defines a FTDC object that can decode binary FTDC data.

    The class is a iterate and take bystring or a Iterator/AsysnIterator as input. The object can be
    iterated upon to get the decoded FTDC data.
    """

    def __init__(
        self, ftdc_data: Union[AsyncIterator, Iterator, ByteString], memory: int = None
    ) -> None:
        """Will initialize FTDC object and validate the provided input."""
        if (
            not isinstance(ftdc_data, Iterator)
            and not isinstance(ftdc_data, bytes)
            and not isinstance(ftdc_data, AsyncIterator)
        ):
            raise ValueError(
                "Invalid FTDC data. The FTDC data should either be an"
                " Iterator/AsyncIterator or ByteString"
            )
        self.raw = ftdc_data
        self.memory = memory

    async def __aiter__(self) -> AsyncGenerator[Dict[Tuple, int], None]:
        """Will aysnc iterate on the byte string and returing a generator with decoded data."""
        if not isinstance(self.raw, AsyncIterator):
            raise ValueError(
                "Async interation not supported. FTDC data is not of type AsyncIterator"
            )
        chunk = await self.raw.__anext__()
        next_chunk = self.raw.__anext__()
        chunks_created, chunks_awaited = 2, 1
        gen = self.getftdc(chunk)
        more = True
        for x, doc_len, buf in gen:
            if x is not None:
                yield x
            while self.memory is None:
                if doc_len is not None:
                    self.memory = 5 * doc_len
                else:
                    x, doc_len, buf = gen.send(await next_chunk)
                    chunks_awaited += 1
                    next_chunk = self.raw.__anext__()
                    chunks_created += 1
                    if x is not None:
                        yield x
            if doc_len is not None and self.memory < doc_len:
                raise Exception(
                    f"doc_len {doc_len} too large to fit into memory {self.memory}"
                )
            if more and (doc_len is None or buf < self.memory):
                try:
                    x, doc_len, buf = gen.send(await next_chunk)
                    chunks_awaited += 1
                    next_chunk = self.raw.__anext__()
                    chunks_created += 1
                    if x is not None:
                        yield x
                    while buf < self.memory:
                        x, doc_len, buf = gen.send(await next_chunk)
                        if x is not None:
                            yield x
                        chunks_awaited += 1
                        next_chunk = self.raw.__anext__()
                        chunks_created += 1
                except StopAsyncIteration:
                    more = False
                except Exception:
                    raise

    def __iter__(self) -> Generator[Dict[Tuple, int], None, None]:
        """Will iterate on the byte string and returing a generator with decoded data."""
        if isinstance(self.raw, bytes):
            gen = self.getftdc(self.raw)
            for x, _doc_len, _buf in gen:
                if x is not None:
                    yield x
        elif isinstance(self.raw, Iterator):
            chunk = next(self.raw)
            gen = self.getftdc(chunk)
            more = True
            for x, doc_len, buf in gen:
                if x is not None:
                    yield x
                while self.memory is None:
                    if doc_len is not None:
                        self.memory = 5 * doc_len
                    else:
                        x, doc_len, buf = gen.send(next(self.raw))
                        if x is not None:
                            yield x
                if doc_len is not None and self.memory < doc_len:
                    raise Exception(
                        f"doc_len {doc_len} too large to fit into memory {self.memory}"
                    )
                if more and (doc_len is None or buf < self.memory):
                    try:
                        x, doc_len, buf = gen.send(next(self.raw))
                        while buf < self.memory:
                            if x is not None:
                                yield x
                            x, doc_len, buf = gen.send(next(self.raw))
                        if x is not None:
                            yield x
                    except StopIteration:
                        more = False
                    except Exception:
                        raise

    def getftdc(
        self, chunk: Union[Chunk, bytes]
    ) -> Generator[
        Tuple[Union[Dict[Tuple, int], None], Union[None, int], int], Chunk, None
    ]:
        """
        Will take a BSON chuck or bytes string as input and decode it.

        Args:
            chunk: This is byststring or chuck that needs to be decoded

        Returns:
            A generator key values pairs of the decoded data.
        """
        frame = bytearray()
        frame.extend(chunk)
        while True:
            chunk_doc, frame = self._read_bson_doc(frame)
            if b"type" in chunk_doc and chunk_doc[b"type"] == 1:
                for x in self._decode_chunk(chunk_doc):
                    chunk = yield x, chunk_doc.bson_len, len(frame)
                    if chunk is not None:
                        frame.extend(chunk)
            elif "bson_len" in chunk_doc:
                chunk = yield None, chunk_doc.bson_len, len(frame)
                if chunk is not None:
                    frame.extend(chunk)
                else:
                    break
            else:
                chunk = yield None, None, len(frame)
                if chunk is not None:
                    frame.extend(chunk)
                else:
                    break

    def _read_bson_doc(
        self, frame: bytearray, ftdc: bool = False
    ) -> Tuple[Chunk, bytearray]:
        if len(frame) < 4:
            return Chunk(), frame
        doc_len = _int32.unpack_from(frame, 0)[0]
        doc = Chunk()
        doc.bson_len = doc_len
        while len(frame) < doc_len:
            return doc, frame
        next_frame = frame[doc_len:]
        frame = frame[4:doc_len]
        while len(frame) > 0:
            bson_type = frame[0]

            frame = frame[1:]
            name_end = frame.find(b"\0", 0)
            n = frame[0:name_end]
            frame = frame[name_end + 1 :]
            if bson_type == 0:  # eoo
                return doc, next_frame
            elif bson_type == 1:  # _double
                v = _double.unpack_from(frame, 0)[0]
                if ftdc:
                    v = int(v)
                frame = frame[8:]
            elif bson_type == 2:  # string
                p = _uint32.unpack_from(frame, 0)[0]
                frame = frame[4:]
                v = frame[: p - 1] if not ftdc else None
                frame = frame[p:]
            elif bson_type == 3:  # subdoc
                v, _ = self._read_bson_doc(frame, ftdc=ftdc)
                frame = frame[v.bson_len :]
            elif bson_type == 4:  # array
                v, _ = self._read_bson_doc(frame, ftdc=ftdc)
                frame = frame[v.bson_len :]

                if not ftdc:
                    v = list(v.values())  # return as array
            elif bson_type == 8:  # bool
                v = frame[0]
                frame = frame[1:]
            elif bson_type == 5:  # bindata
                p = _uint32.unpack_from(frame, 0)[0]
                frame = frame[5:]
                v = frame[:p] if not ftdc else None
                frame = frame[p:]
            elif bson_type == 7:  # objectid
                v = None  # xxx always ignore for now
                frame = frame[12:]
            elif bson_type == 9:  # datetime
                v = _uint64.unpack_from(frame, 0)[0]
                v = int(v) if ftdc else v / 1000.0
                frame = frame[8:]
            elif bson_type == 16:  # _int32
                v = _int32.unpack_from(frame, 0)[0]
                if ftdc:
                    v = int(v)
                frame = frame[4:]
            elif bson_type == 17:  # timestamp
                v = Chunk()
                v[b"t"] = int(_uint32.unpack_from(frame, 0)[0])  # seconds
                v[b"i"] = int(_uint32.unpack_from(frame, 4)[0])  # increment
                frame = frame[8:]
            elif bson_type == 18:  # _int64
                v = int(_int64.unpack_from(frame, 0)[0])
                frame = frame[8:]
            elif bson_type == 0xFF or bson_type == 0x7F:  # minkey, maxkey
                v = None  # xxx always ignore for now
                p = 0
            else:
                err_msg = "unknown type %d(%x) at %d(%x)"
                raise Exception(err_msg % (bson_type, bson_type))
            if v is not None:
                doc[bytes(n)] = v
        # assert not "eoo not found"  # should have seen an eoo and returned
        return doc, next_frame  # mypy cannot resolve this as unreachable

    def _decode_chunk(self, chunk_doc: Chunk) -> Generator[dict, None, None]:
        # our result is a map from metric keys to list of values for each metric key
        # a metric key is a path through the sample document represented as a tuple
        metrics = Chunk()

        # decompress chunk data field
        data = chunk_doc[b"data"]
        metrics.chunk_len = len(data)
        data = data[4:]  # skip uncompressed length, we don't need it
        data = zlib.decompress(data)
        # read reference doc from chunk data, ignoring non-metric fields
        ref_doc, _ = self._read_bson_doc(bytearray(data), ftdc=True)

        metrics_init = {}

        # traverse the reference document and extract map from metrics keys to values
        def extract_keys(doc: Chunk, n: Tuple = ()) -> None:
            for k, v in doc.items():
                nn = n + (k,)
                if type(v) == Chunk:
                    extract_keys(v, nn)
                else:
                    metrics_init[nn] = v

        extract_keys(ref_doc)
        if len(metrics_init.keys()) == 0:
            return
        else:
            # get nmetrics, ndeltas
            nmetrics = _uint32.unpack_from(data, ref_doc.bson_len)[0]
            ndeltas = _uint32.unpack_from(data, ref_doc.bson_len + 4)[0]
            nsamples = ndeltas + 1
            at = ref_doc.bson_len + 8
            if nmetrics != len(metrics_init):
                raise RuntimeError(
                    "Bad chunk found, but application never supported it"
                )
            metrics.nsamples = nsamples

            # unpacks ftdc packed ints
            def unpack(data: ByteString, at: int) -> Tuple[int, int]:
                res = 0
                shift = 0
                while True:
                    b = data[at]
                    res |= (b & 0x7F) << shift
                    at += 1
                    if not (b & 0x80):
                        if res > 0x7FFFFFFFFFFFFFFF:  # negative 64-bit value
                            res = int(res - 0x10000000000000000)
                        return res, at
                    shift += 7

            # unpack, run-length, delta, transpose the metrics
            nzeroes = 0
            ftdc_record: Dict[int, Dict] = {}
            yield metrics_init
            for k, v in metrics_init.items():
                value = v
                for i in range(ndeltas):
                    if nzeroes:
                        delta = 0
                        nzeroes -= 1
                    else:
                        delta, at = unpack(data, at)
                        if delta == 0:
                            nzeroes, at = unpack(data, at)
                    value += delta
                    if i + 1 not in ftdc_record:
                        ftdc_record[i + 1] = {}
                    ftdc_record[i + 1][k] = value

            for x in ftdc_record.values():
                yield x
