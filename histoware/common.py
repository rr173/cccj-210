"""Shared validation and canonicalisation for histoware.

Numeric policy
--------------
* Bucket counts and totals may reach 2**64 - 1, and aggregate sums may
  exceed it.  They are therefore kept as Python integers of arbitrary
  precision and stored in SQLite as decimal TEXT.  Nothing ever goes
  through binary floating point, and no count is ever placed in a
  SQLite signed 64-bit INTEGER column.
* Timestamps (whole UTC seconds), revision numbers and millisecond cut
  points must fit the signed 64-bit range; they are stored as SQLite
  INTEGERs.
"""
from __future__ import annotations

import json
from typing import NamedTuple

MAX_COUNT = (1 << 64) - 1          # counts / totals may reach 2^64 - 1
MAX_INT64 = (1 << 63) - 1          # ts / revision / cut points

TOOL_VERSION = "1.0.0"


class HistowareError(Exception):
    """Base class for user-facing errors."""


class LineError(HistowareError):
    """A single input line is invalid."""

    def __init__(self, line_no: int, message: str):
        super().__init__(f"line {line_no}: {message}")
        self.line_no = line_no
        self.message = message


def _object_no_dup(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError(f"duplicate key {key!r}")
        obj[key] = value
    return obj


def load_json_object(raw, line_no: int) -> dict:
    """Parse one input line into a JSON object, rejecting blank lines,
    duplicate keys and non-object documents."""
    try:
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
    except UnicodeDecodeError as exc:
        raise LineError(line_no, f"invalid UTF-8: {exc}") from exc
    text = text.strip()
    if not text:
        raise LineError(line_no, "blank line")
    try:
        obj = json.loads(text, object_pairs_hook=_object_no_dup)
    except ValueError as exc:
        raise LineError(line_no, f"invalid JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise LineError(line_no, "expected a JSON object")
    return obj


def _check_keys(obj: dict, allowed: set, line_no: int) -> None:
    keys = set(obj)
    if keys != allowed:
        parts = []
        missing = sorted(allowed - keys)
        extra = sorted(keys - allowed)
        if missing:
            parts.append("missing " + ", ".join(repr(k) for k in missing))
        if extra:
            parts.append("unexpected " + ", ".join(repr(k) for k in extra))
        raise LineError(line_no, "bad fields: " + "; ".join(parts))


def _req_str(obj: dict, key: str, line_no: int) -> str:
    value = obj[key]
    if type(value) is not str or value == "":
        raise LineError(line_no, f"{key!r} must be a non-empty string")
    return value


def _req_int(obj: dict, key: str, line_no: int, lo: int, hi: int) -> int:
    value = obj[key]
    # type(x) is int also rejects bool (a subclass of int) and floats.
    if type(value) is not int or not (lo <= value <= hi):
        raise LineError(line_no, f"{key!r} must be an integer in [{lo}, {hi}]")
    return value


class Checkpoint(NamedTuple):
    """One validated checkpoint line, in canonical form."""
    region: str
    collector: str
    generation: str
    ts: int
    revision: int
    cuts: tuple      # strictly increasing positive ints (ms)
    counts: tuple    # parallel to cuts, non-decreasing, each <= total
    total: int

    @property
    def cuts_json(self) -> str:
        return json.dumps(self.cuts, separators=(",", ":"))

    @property
    def counts_json(self) -> str:
        return json.dumps(self.counts, separators=(",", ":"))

    @property
    def total_str(self) -> str:
        return str(self.total)


_CHECKPOINT_KEYS = {
    "region", "collector", "generation", "ts", "revision",
    "cuts_ms", "counts", "total",
}


def parse_checkpoint(raw, line_no: int) -> Checkpoint:
    obj = load_json_object(raw, line_no)
    _check_keys(obj, _CHECKPOINT_KEYS, line_no)
    region = _req_str(obj, "region", line_no)
    collector = _req_str(obj, "collector", line_no)
    generation = _req_str(obj, "generation", line_no)
    ts = _req_int(obj, "ts", line_no, 0, MAX_INT64)
    revision = _req_int(obj, "revision", line_no, 0, MAX_INT64)
    cuts = obj["cuts_ms"]
    counts = obj["counts"]
    if type(cuts) is not list or type(counts) is not list:
        raise LineError(line_no, "'cuts_ms' and 'counts' must be arrays")
    if len(cuts) != len(counts):
        raise LineError(line_no, "'cuts_ms' and 'counts' must have equal length")
    prev = 0
    for cut in cuts:
        if type(cut) is not int or not (1 <= cut <= MAX_INT64):
            raise LineError(line_no, "cut points must be integers in [1, 2^63-1]")
        if cut <= prev:
            raise LineError(line_no, "'cuts_ms' must be strictly increasing")
        prev = cut
    prev = -1
    for count in counts:
        if type(count) is not int or not (0 <= count <= MAX_COUNT):
            raise LineError(line_no, "counts must be integers in [0, 2^64-1]")
        if count < prev:
            raise LineError(line_no, "'counts' must be non-decreasing")
        prev = count
    total = _req_int(obj, "total", line_no, 0, MAX_COUNT)
    if counts and counts[-1] > total:
        raise LineError(line_no, "'total' must be >= the largest bucket count")
    return Checkpoint(region, collector, generation, ts, revision,
                      tuple(cuts), tuple(counts), total)


class Duty(NamedTuple):
    """One duty-roster row: [start, end) half-open, whole seconds."""
    region: str
    collector: str
    generation: str
    start: int
    end: int


_DUTY_KEYS = {"region", "collector", "generation", "start", "end"}


def parse_duty(raw, line_no: int) -> Duty:
    obj = load_json_object(raw, line_no)
    _check_keys(obj, _DUTY_KEYS, line_no)
    region = _req_str(obj, "region", line_no)
    collector = _req_str(obj, "collector", line_no)
    generation = _req_str(obj, "generation", line_no)
    start = _req_int(obj, "start", line_no, 0, MAX_INT64)
    end = _req_int(obj, "end", line_no, 0, MAX_INT64)
    if start >= end:
        raise LineError(line_no, "'start' must be < 'end'")
    return Duty(region, collector, generation, start, end)


class ShiftRequest(NamedTuple):
    """One named shift to summarise."""
    name: str
    region: str
    start: int
    end: int
    cuts: tuple      # sorted unique positive ints (ms); may be empty


_SHIFT_KEYS = {"name", "region", "start", "end", "cuts_ms"}


def parse_shift(raw, line_no: int) -> ShiftRequest:
    obj = load_json_object(raw, line_no)
    _check_keys(obj, _SHIFT_KEYS, line_no)
    name = _req_str(obj, "name", line_no)
    region = _req_str(obj, "region", line_no)
    start = _req_int(obj, "start", line_no, 0, MAX_INT64)
    end = _req_int(obj, "end", line_no, 0, MAX_INT64)
    if start >= end:
        raise LineError(line_no, "'start' must be < 'end'")
    cuts = obj["cuts_ms"]
    if type(cuts) is not list:
        raise LineError(line_no, "'cuts_ms' must be an array")
    for cut in cuts:
        if type(cut) is not int or not (1 <= cut <= MAX_INT64):
            raise LineError(line_no, "query cut points must be integers in [1, 2^63-1]")
    return ShiftRequest(name, region, start, end, tuple(sorted(set(cuts))))
