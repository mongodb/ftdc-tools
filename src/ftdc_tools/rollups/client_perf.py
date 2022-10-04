"""Statistics calculations for client-side performance data."""

from datetime import timezone
from dataclasses import dataclass
from typing import AsyncIterator, ByteString, Dict, Iterator, List, Tuple, Union

from scipy.stats.mstats import mquantiles

TO_NANOSECONDS = 1e9
NANO_TO_MILLISECONDS = 1e6


@dataclass
class Statistic:
    """A statistic."""

    name: str
    value: float
    version: int
    user_submitted: bool


class ClientPerformanceStatistics:
    """
    Load client-side perf statistics.

    These statistics are used in Poplar, Genny, and Data Pipes to represent a
    time series of client-observable events.
    """

    def __init__(
        self, ftdc_data: Union[AsyncIterator, Iterator, ByteString], memory: int = None
    ) -> None:
        """Initialize the class."""
        self._operations_total = 0
        self._documents_total = 0
        self._size_total = 0
        self._errors_total = 0
        self._duration_total = 0
        self._extracted_durations: List[float] = []
        self._wall_time_total = 0.0
        self._timers_total = 0
        self._gauges_workers_min = 0
        self._gauges_workers_max = 0
        self.first_record: Dict[Tuple, int] = {}
        self.last_record: Dict[Tuple, int] = {}
        self._ftdc_data = ftdc_data
        self._memory = memory
        self._processed = False
        self._min_duration = 0.0
        self._max_duration = 0.0


    def _process(self) -> None:
        """
        Process the FTDC file and gather the statistics.

        :return: None.
        """
        if self._processed:
            return
        try:
            previous_duration = 0.0
            for record in self._ftdc_data:
                if "dur" in record["timers"].keys():
                    duration = float(record["timers"]["dur"])
                else:
                    duration = float(record["timers"]["duration"])
                extracted_duration = duration - previous_duration
                if not self.first_record:
                    self._min_duration = extracted_duration
                    self._max_duration = extracted_duration
                else:
                    self._min_duration = min(self._min_duration, extracted_duration)
                    self._max_duration = max(self._max_duration, extracted_duration)
                start_ts = _ts_to_milliseconds(record["ts"]) - (extracted_duration) / NANO_TO_MILLISECONDS
                previous_duration = duration
                if not self.first_record or self.first_record["start_ts"] > start_ts:
                    record = record.copy()
                    record["start_ts"] = start_ts
                    self.first_record = record

                self.last_record = record
                self._gauges_workers_min = min(
                    self._gauges_workers_min, record["gauges"]["workers"]
                )
                self._gauges_workers_max = min(
                    self._gauges_workers_max, record["gauges"]["workers"]
                )
                self._extracted_durations.append(extracted_duration)

            if not self.first_record and not self.last_record:
                return
            self._operations_total = self.last_record["counters"]["ops"]
            self._documents_total = self.last_record["counters"]["n"]
            self._size_total = self.last_record["counters"]["size"]
            self._errors_total = self.last_record["counters"]["errors"]
            self._timers_total = self.last_record["timers"]["total"]
            if "dur" in self.last_record["timers"]:
                self._duration_total = self.last_record["timers"]["dur"]
            else:
                self._duration_total = self.last_record["timers"]["duration"]
            if self.last_record and self.first_record:
                time_diff = _ts_to_milliseconds(self.last_record["ts"]) - self.first_record["start_ts"]
                self._wall_time_total = time_diff / 1000
        except KeyError as err:
            raise ValueError(f"Missing field - {err} - duration in Genny client side FTDC record.")
        self._processed = True

    @property
    def all_statistics(self) -> List[Statistic]:
        """
        Get all the statistics.

        :return: All the statistics.
        """
        self._process()
        return [
            self.average_latency,
            self.average_size,
            self.operation_throughput,
            self.document_throughput,
            self.error_rate,
            self.size_throughput,
            self.workers_min,
            self.workers_max,
            self.latency_min,
            self.latency_max,
            self.duration_total,
            self.errors_total,
            self.operations_total,
            self.documents_total,
            self.size_total,
            self.overhead_total,
        ] + self.latency_quantiles

    @property
    def average_latency(self) -> Statistic:
        """
        Get average latency.

        :return: Average latency.
        """
        version = 3
        return Statistic(
            "AverageLatency",
            self._duration_total / self._operations_total if self._operations_total > 0 else 0,
            version,
            False,
        )

    @property
    def average_size(self) -> Statistic:
        """
        Get average size.

        :return: Average size.
        """
        version = 3
        return Statistic(
            "AverageSize",
            self._size_total / self._operations_total if self._operations_total > 0 else 0,
            version,
            False,
        )

    @property
    def operation_throughput(self) -> Statistic:
        """
        Get operation throughput.

        :return: Operation throughput.
        """
        version = 5
        return Statistic(
            "OperationThroughput",
            self._operations_total / self._wall_time_total
            if self._wall_time_total > 0
            else self._operations_total,
            version,
            False,
        )

    @property
    def document_throughput(self) -> Statistic:
        """
        Get document throughput.

        :return: Document throughput.
        """
        version = 1
        return Statistic(
            "DocumentThroughput",
            self._documents_total / self._wall_time_total
            if self._wall_time_total > 0
            else self._documents_total,
            version,
            False,
        )

    @property
    def error_rate(self) -> Statistic:
        """
        Get error rate.

        :return: Error rate.
        """
        version = 5
        return Statistic(
            "ErrorRate",
            self._errors_total / self._wall_time_total
            if self._wall_time_total > 0
            else self._errors_total,
            version,
            False,
        )

    @property
    def size_throughput(self) -> Statistic:
        """
        Get size throughput.

        :return: Size throughput.
        """
        version = 5
        return Statistic(
            "SizeThroughput",
            self._size_total / self._wall_time_total
            if self._wall_time_total > 0
            else self._size_total,
            version,
            False,
        )

    @property
    def latency_quantiles(self) -> List[Statistic]:
        """
        Get latency quantiles.

        :return: Latency quantiles.
        """
        version = 4
        quantile_names = [
            "Latency50thPercentile",
            "Latency80thPercentile",
            "Latency90thPercentile",
            "Latency95thPercentile",
            "Latency99thPercentile",
        ]
        quantiles = [0, 0, 0, 0, 0]
        if len(self._extracted_durations) > 0:
            quantiles = mquantiles(
                self._extracted_durations,
                prob=[0.5, 0.8, 0.9, 0.95, 0.99],
                alphap=1 / 3,
                betap=1 / 3,
            )
        all_quantiles = []
        for name, value in zip(quantile_names, quantiles):
            all_quantiles.append(Statistic(name, value, version, False))
        return all_quantiles

    @property
    def workers_min(self) -> Statistic:
        """
        Get minimum workers.

        :return: Minimum workers.
        """
        version = 3
        return Statistic(
            "WorkersMin",
            self._gauges_workers_min,
            version,
            False,
        )

    @property
    def workers_max(self) -> Statistic:
        """
        Get maximum workers.

        :return: Maximum workers.
        """
        version = 3
        return Statistic(
            "WorkersMax",
            self._gauges_workers_max,
            version,
            False,
        )

    @property
    def latency_min(self) -> Statistic:
        """
        Get minimum latency.

        :return: Minimum latency.
        """
        version = 4
        return Statistic(
            "LatencyMin",
            self._min_duration if len(self._extracted_durations) > 0 else 0,
            version,
            False,
        )

    @property
    def latency_max(self) -> Statistic:
        """
        Get maximum latency.

        :return: Maximum latency.
        """
        version = 4
        return Statistic(
            "LatencyMax",
            self._max_duration if len(self._extracted_durations) > 0 else 0,
            version,
            False,
        )

    @property
    def duration_total(self) -> Statistic:
        """
        Get total duration.

        :return: Total duration.
        """
        version = 5
        # Though duration total should have been sum of duration, the wall time is used below to
        # keep things consistent with legacy cedar.
        return Statistic(
            "DurationTotal",
            self._wall_time_total * TO_NANOSECONDS,
            version,
            False,
        )

    @property
    def errors_total(self) -> Statistic:
        """
        Get total errors.

        :return: Total errors.
        """
        version = 3
        return Statistic(
            "ErrorsTotal",
            self._errors_total,
            version,
            False,
        )

    @property
    def operations_total(self) -> Statistic:
        """
        Get total operations.

        :return: Total operations.
        """
        version = 3
        return Statistic(
            "OperationsTotal",
            self._operations_total,
            version,
            False,
        )

    @property
    def documents_total(self) -> Statistic:
        """
        Get total documents.

        :return: Total documents.
        """
        version = 0
        return Statistic(
            "DocumentsTotal",
            self._documents_total,
            version,
            False,
        )

    @property
    def size_total(self) -> Statistic:
        """
        Get total size.

        :return: Total size.
        """
        version = 3
        return Statistic(
            "SizeTotal",
            self._size_total,
            version,
            False,
        )

    @property
    def overhead_total(self) -> Statistic:
        """
        Get total overhead.

        :return: Total overhead.
        """
        version = 1
        return Statistic(
            "OverheadTotal",
            self._timers_total - self._duration_total,
            version,
            False,
        )

def _ts_to_milliseconds(ts):
    return ts.replace(tzinfo=timezone.utc).timestamp() * 1000
