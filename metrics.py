"""Small, dependency-free Prometheus metrics primitives.

The project intentionally keeps observability usable with only the Python standard
library.  This module implements the subset of the Prometheus text exposition format
needed by the KV node: counters, gauges, and histograms with fixed, low-cardinality
label sets.

Metric values are process-local and reset when a node restarts.  Prometheus (or another
scraper) is responsible for retaining time-series history across restarts.
"""

from __future__ import annotations

import math
import threading
from collections import defaultdict
from typing import Iterable


def _escape_help(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n")


def _escape_label(value: object) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace('"', '\\"')
    )


def _format_number(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    if value == int(value):
        return str(int(value))
    return format(value, ".15g")


class _Metric:
    metric_type = "untyped"

    def __init__(self, name: str, help_text: str, label_names: Iterable[str] = ()):
        if not name or not all(char.isalnum() or char in "_:" for char in name):
            raise ValueError(f"invalid metric name: {name!r}")
        self.name = name
        self.help_text = help_text
        self.label_names = tuple(label_names)
        if len(set(self.label_names)) != len(self.label_names):
            raise ValueError(f"duplicate label name for metric {name}")
        self._lock = threading.RLock()

    def _label_values(self, labels: dict[str, object]) -> tuple[str, ...]:
        expected = set(self.label_names)
        received = set(labels)
        if received != expected:
            missing = sorted(expected - received)
            extra = sorted(received - expected)
            raise ValueError(
                f"labels for {self.name} do not match; missing={missing}, extra={extra}"
            )
        return tuple(str(labels[name]) for name in self.label_names)

    def _sample(self, name: str, values: tuple[str, ...], value: float,
                extra_labels: tuple[tuple[str, object], ...] = ()) -> str:
        labels = list(zip(self.label_names, values)) + list(extra_labels)
        if labels:
            rendered = ",".join(
                f'{label_name}="{_escape_label(label_value)}"'
                for label_name, label_value in labels
            )
            return f"{name}{{{rendered}}} {_format_number(value)}"
        return f"{name} {_format_number(value)}"

    def render(self) -> list[str]:
        raise NotImplementedError


class Counter(_Metric):
    metric_type = "counter"

    def __init__(self, name: str, help_text: str, label_names: Iterable[str] = ()):
        super().__init__(name, help_text, label_names)
        self._values: dict[tuple[str, ...], float] = defaultdict(float)

    def inc(self, amount: float = 1.0, **labels: object) -> None:
        amount = float(amount)
        if amount < 0 or not math.isfinite(amount):
            raise ValueError("counter increments must be finite and non-negative")
        values = self._label_values(labels)
        with self._lock:
            self._values[values] += amount

    def render(self) -> list[str]:
        with self._lock:
            items = sorted(self._values.items())
        return [self._sample(self.name, labels, value) for labels, value in items]


class Gauge(_Metric):
    metric_type = "gauge"

    def __init__(self, name: str, help_text: str, label_names: Iterable[str] = ()):
        super().__init__(name, help_text, label_names)
        self._values: dict[tuple[str, ...], float] = {}

    def set(self, value: float, **labels: object) -> None:
        value = float(value)
        values = self._label_values(labels)
        with self._lock:
            self._values[values] = value

    def inc(self, amount: float = 1.0, **labels: object) -> None:
        amount = float(amount)
        if not math.isfinite(amount):
            raise ValueError("gauge increments must be finite")
        values = self._label_values(labels)
        with self._lock:
            self._values[values] = self._values.get(values, 0.0) + amount

    def render(self) -> list[str]:
        with self._lock:
            items = sorted(self._values.items())
        return [self._sample(self.name, labels, value) for labels, value in items]


class Histogram(_Metric):
    metric_type = "histogram"

    def __init__(self, name: str, help_text: str, buckets: Iterable[float],
                 label_names: Iterable[str] = ()):
        super().__init__(name, help_text, label_names)
        normalized = tuple(sorted(set(float(bucket) for bucket in buckets)))
        if not normalized or not all(math.isfinite(bucket) for bucket in normalized):
            raise ValueError("histogram buckets must be non-empty and finite")
        self.buckets = normalized
        self._bucket_counts: dict[tuple[str, ...], list[int]] = {}
        self._counts: dict[tuple[str, ...], int] = defaultdict(int)
        self._sums: dict[tuple[str, ...], float] = defaultdict(float)

    def observe(self, value: float, **labels: object) -> None:
        value = float(value)
        if not math.isfinite(value):
            raise ValueError("histogram observations must be finite")
        label_values = self._label_values(labels)
        with self._lock:
            counts = self._bucket_counts.setdefault(
                label_values, [0 for _ in self.buckets]
            )
            for index, upper_bound in enumerate(self.buckets):
                if value <= upper_bound:
                    counts[index] += 1
            self._counts[label_values] += 1
            self._sums[label_values] += value

    def render(self) -> list[str]:
        with self._lock:
            labels_seen = sorted(self._counts)
            snapshots = [
                (
                    labels,
                    list(self._bucket_counts[labels]),
                    self._counts[labels],
                    self._sums[labels],
                )
                for labels in labels_seen
            ]

        lines = []
        for labels, bucket_counts, count, total in snapshots:
            for upper_bound, bucket_count in zip(self.buckets, bucket_counts):
                lines.append(self._sample(
                    f"{self.name}_bucket",
                    labels,
                    bucket_count,
                    (("le", _format_number(upper_bound)),),
                ))
            lines.append(self._sample(
                f"{self.name}_bucket", labels, count, (("le", "+Inf"),)
            ))
            lines.append(self._sample(f"{self.name}_sum", labels, total))
            lines.append(self._sample(f"{self.name}_count", labels, count))
        return lines


class Registry:
    """Owns metric definitions and renders a consistent scrape snapshot."""

    def __init__(self):
        self._lock = threading.RLock()
        self._metrics: dict[str, _Metric] = {}

    def _register(self, metric: _Metric) -> _Metric:
        with self._lock:
            if metric.name in self._metrics:
                raise ValueError(f"metric already registered: {metric.name}")
            self._metrics[metric.name] = metric
        return metric

    def counter(self, name: str, help_text: str,
                label_names: Iterable[str] = ()) -> Counter:
        return self._register(Counter(name, help_text, label_names))  # type: ignore[return-value]

    def gauge(self, name: str, help_text: str,
              label_names: Iterable[str] = ()) -> Gauge:
        return self._register(Gauge(name, help_text, label_names))  # type: ignore[return-value]

    def histogram(self, name: str, help_text: str, buckets: Iterable[float],
                  label_names: Iterable[str] = ()) -> Histogram:
        return self._register(  # type: ignore[return-value]
            Histogram(name, help_text, buckets, label_names)
        )

    def render(self) -> str:
        with self._lock:
            metrics = [self._metrics[name] for name in sorted(self._metrics)]

        lines = []
        for metric in metrics:
            lines.append(f"# HELP {metric.name} {_escape_help(metric.help_text)}")
            lines.append(f"# TYPE {metric.name} {metric.metric_type}")
            lines.extend(metric.render())
        return "\n".join(lines) + "\n"
