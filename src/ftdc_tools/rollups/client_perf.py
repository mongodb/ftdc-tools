"""Statistics calculations for client-side performance data."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from scipy.stats.mstats import mquantiles

from ftdc_tools.decode import FTDCDoc

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

    def __init__(self) -> None:
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
        self.first_doc: Optional[FTDCDoc] = None
        self.last_doc: Optional[FTDCDoc] = None
        self.previous_duration = 0.0
        self._min_duration = 0.0
        self._max_duration = 0.0
        self._finalized = False

    def add_doc(self, doc: FTDCDoc) -> None:
        """Add a doc to the rollup."""
        self._finalized = False

        if "dur" in doc["timers"].keys():
            duration = float(doc["timers"]["dur"])
        else:
            duration = float(doc["timers"]["duration"])
        extracted_duration = duration - self.previous_duration

        if not self.first_doc:
            self._min_duration = extracted_duration
            self._max_duration = extracted_duration
            self.first_doc = doc
        else:
            self._min_duration = min(self._min_duration, extracted_duration)
            self._max_duration = max(self._max_duration, extracted_duration)

        self.previous_duration = duration

        self.last_doc = doc
        self._gauges_workers_min = min(
            self._gauges_workers_min, doc["gauges"]["workers"]
        )
        self._gauges_workers_max = min(
            self._gauges_workers_max, doc["gauges"]["workers"]
        )
        self._extracted_durations.append(extracted_duration)

    @property
    def all_statistics(self) -> List[Statistic]:
        """
        Get all the statistics.

        :return: All the statistics.
        """
        self._finalize()
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
        self._finalize()
        version = 3
        return Statistic(
            "AverageLatency",
            self._duration_total / self._operations_total
            if self._operations_total > 0
            else 0,
            version,
            False,
        )

    @property
    def average_size(self) -> Statistic:
        """
        Get average size.

        :return: Average size.
        """
        self._finalize()
        version = 3
        return Statistic(
            "AverageSize",
            self._size_total / self._operations_total
            if self._operations_total > 0
            else 0,
            version,
            False,
        )

    @property
    def operation_throughput(self) -> Statistic:
        """
        Get operation throughput.

        :return: Operation throughput.
        """
        self._finalize()
        version = 4
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
        self._finalize()
        version = 0
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
        self._finalize()
        version = 4
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
        self._finalize()
        version = 4
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
        self._finalize()
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
        self._finalize()
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
        self._finalize()
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
        self._finalize()
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
        self._finalize()
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
        self._finalize()
        version = 4
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
        self._finalize()
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
        self._finalize()
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
        self._finalize()
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
        self._finalize()
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
        self._finalize()
        version = 1
        return Statistic(
            "OverheadTotal",
            self._timers_total - self._duration_total,
            version,
            False,
        )

    def _finalize(self) -> None:
        """
        Finalize values used across all calculations.

        This is safe to call multiple times, will not act if redundant.
        """
        if self._finalized:
            return
        if self.first_doc is None or self.last_doc is None:
            return
        self._operations_total = self.last_doc["counters"]["ops"]
        self._documents_total = self.last_doc["counters"]["n"]
        self._size_total = self.last_doc["counters"]["size"]
        self._errors_total = self.last_doc["counters"]["errors"]
        self._timers_total = self.last_doc["timers"]["total"]
        if "dur" in self.last_doc["timers"]:
            self._duration_total = self.last_doc["timers"]["dur"]
        else:
            self._duration_total = self.last_doc["timers"]["duration"]
        if self.last_doc and self.first_doc:
            time_diff = _ts_to_milliseconds(self.last_doc["ts"]) - _ts_to_milliseconds(
                self.first_doc["ts"]
            )
            self._wall_time_total = time_diff / 1000
        self._finalized = True


def _ts_to_milliseconds(ts: datetime) -> int:
    return int(ts.replace(tzinfo=timezone.utc).timestamp() * 1000)
