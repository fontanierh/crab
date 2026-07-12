#!/usr/bin/env python3
"""Parse LCOV line totals without mistaking omitted zero-hit rows for coverage."""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

sys.dont_write_bytecode = True

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.workflow_common import (
    COVERAGE_IGNORE_FILENAME_REGEX,
    WorkflowError,
    repository_root,
)


@dataclass(frozen=True)
class FileStats:
    path: str
    lines_found: int
    lines_hit: int
    zero_hit_lines: tuple[int, ...]
    locations_complete: bool
    totals_from_da: bool

    @property
    def uncovered_lines(self) -> int:
        return self.lines_found - self.lines_hit


@dataclass(frozen=True)
class LcovStats:
    files: tuple[FileStats, ...]

    @property
    def lines_found(self) -> int:
        return sum(item.lines_found for item in self.files)

    @property
    def lines_hit(self) -> int:
        return sum(item.lines_hit for item in self.files)

    @property
    def uncovered_lines(self) -> int:
        return self.lines_found - self.lines_hit

    @property
    def uncovered_files(self) -> int:
        return sum(item.uncovered_lines > 0 for item in self.files)


@dataclass
class _Record:
    source: str
    hits: dict[int, int]
    lines_found: int | None = None
    lines_hit: int | None = None


def _integer(value: str, *, field: str, source: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise WorkflowError(
            f"LCOV {field} for {source} is not an integer: {value!r}; regenerate coverage"
        ) from error
    if parsed < 0:
        raise WorkflowError(
            f"LCOV {field} for {source} is negative: {parsed}; regenerate coverage"
        )
    return parsed


def _normalize_source(root: Path, raw: str) -> str:
    if not raw:
        raise WorkflowError("LCOV contains an empty SF path; regenerate coverage")
    root = root.resolve()
    source = Path(raw)
    candidate = source if source.is_absolute() else root / source
    resolved = candidate.resolve(strict=False)
    try:
        relative = resolved.relative_to(root).as_posix()
    except ValueError as error:
        raise WorkflowError(
            f"LCOV source is outside the repository: {raw}; regenerate coverage"
        ) from error
    if re.search(COVERAGE_IGNORE_FILENAME_REGEX, relative):
        raise WorkflowError(
            f"LCOV contains policy-excluded source {relative}; regenerate coverage"
        )
    return relative


def _finish_record(record: _Record) -> None:
    if (record.lines_found is None) != (record.lines_hit is None):
        raise WorkflowError(
            f"LCOV {record.source} has LF without LH (or vice versa); regenerate coverage"
        )
    if record.lines_found is None:
        return
    assert record.lines_hit is not None
    if record.lines_hit > record.lines_found:
        raise WorkflowError(
            f"LCOV {record.source} has LH greater than LF; regenerate coverage"
        )
    if len(record.hits) > record.lines_found:
        raise WorkflowError(
            f"LCOV {record.source} has more distinct DA rows than LF; regenerate coverage"
        )
    zero_rows = sum(hits == 0 for hits in record.hits.values())
    positive_rows = len(record.hits) - zero_rows
    if len(record.hits) == record.lines_found and (
        positive_rows != record.lines_hit
        or zero_rows != record.lines_found - record.lines_hit
    ):
        raise WorkflowError(
            f"LCOV DA rows disagree with LF/LH for {record.source}; regenerate coverage"
        )
    if positive_rows > record.lines_hit:
        raise WorkflowError(
            f"LCOV positive DA rows exceed LH for {record.source}; regenerate coverage"
        )
    if zero_rows > record.lines_found - record.lines_hit:
        raise WorkflowError(
            f"LCOV DA rows disagree with LF/LH for {record.source}; regenerate coverage"
        )


def parse_lcov(root: Path, path: Path) -> LcovStats:
    """Return per-file and aggregate line statistics from a fresh LCOV artifact."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise WorkflowError(f"could not read LCOV artifact {path}: {error}") from error

    records: list[_Record] = []
    current: _Record | None = None
    for raw in lines:
        if raw.startswith("SF:"):
            if current is not None:
                raise WorkflowError(
                    f"LCOV record for {current.source} is missing end_of_record; regenerate coverage"
                )
            current = _Record(_normalize_source(root, raw[3:]), {})
        elif raw.startswith("DA:"):
            if current is None:
                raise WorkflowError("LCOV DA row appears outside an SF record; regenerate coverage")
            fields = raw[3:].split(",")
            if len(fields) < 2:
                raise WorkflowError(
                    f"LCOV DA row for {current.source} is malformed: {raw!r}; regenerate coverage"
                )
            line = _integer(fields[0], field="DA line", source=current.source)
            hits = _integer(fields[1], field="DA hits", source=current.source)
            if line == 0:
                raise WorkflowError(
                    f"LCOV DA line for {current.source} must be positive; regenerate coverage"
                )
            current.hits[line] = current.hits.get(line, 0) + hits
        elif raw.startswith("LF:"):
            if current is None:
                raise WorkflowError("LCOV LF row appears outside an SF record; regenerate coverage")
            if current.lines_found is not None:
                raise WorkflowError(
                    f"LCOV {current.source} repeats LF in one record; regenerate coverage"
                )
            current.lines_found = _integer(
                raw[3:], field="LF", source=current.source
            )
        elif raw.startswith("LH:"):
            if current is None:
                raise WorkflowError("LCOV LH row appears outside an SF record; regenerate coverage")
            if current.lines_hit is not None:
                raise WorkflowError(
                    f"LCOV {current.source} repeats LH in one record; regenerate coverage"
                )
            current.lines_hit = _integer(raw[3:], field="LH", source=current.source)
        elif raw == "end_of_record":
            if current is None:
                raise WorkflowError("LCOV has end_of_record without SF; regenerate coverage")
            _finish_record(current)
            records.append(current)
            current = None
    if current is not None:
        raise WorkflowError(
            f"LCOV record for {current.source} is missing end_of_record; regenerate coverage"
        )
    if not records:
        raise WorkflowError(f"LCOV artifact {path} contains no source records; regenerate coverage")

    grouped: dict[str, list[_Record]] = defaultdict(list)
    for record in records:
        grouped[record.source].append(record)

    files: list[FileStats] = []
    for source, source_records in sorted(grouped.items()):
        if len(source_records) == 1:
            record = source_records[0]
            totals_from_da = record.lines_found is None
            lines_found = (
                len(record.hits) if totals_from_da else record.lines_found
            )
            lines_hit = (
                sum(hits > 0 for hits in record.hits.values())
                if totals_from_da
                else record.lines_hit
            )
            assert lines_found is not None and lines_hit is not None
            zero_hit_lines = tuple(
                sorted(line for line, hits in record.hits.items() if hits == 0)
            )
            files.append(
                FileStats(
                    source,
                    lines_found,
                    lines_hit,
                    zero_hit_lines,
                    lines_found - lines_hit == len(zero_hit_lines),
                    totals_from_da,
                )
            )
            continue

        for record in source_records:
            if record.lines_found is not None and len(record.hits) != record.lines_found:
                raise WorkflowError(
                    f"duplicate LCOV records for {source} omit DA rows, so their line universe "
                    "cannot be merged; regenerate coverage"
                )
        merged: dict[int, int] = defaultdict(int)
        # Merge line hits only after every record's totals proved self-consistent.
        # A duplicate record must never launder contradictory LF/LH evidence.
        for record in source_records:
            for line, hits in record.hits.items():
                merged[line] += hits
        zero_hit_lines = tuple(sorted(line for line, hits in merged.items() if hits == 0))
        files.append(
            FileStats(
                source,
                len(merged),
                sum(hits > 0 for hits in merged.values()),
                zero_hit_lines,
                True,
                True,
            )
        )
    return LcovStats(tuple(files))


def hotspot_markdown(stats: LcovStats, *, limit: int = 15) -> str:
    rows = sorted(
        (item for item in stats.files if item.uncovered_lines > 0),
        key=lambda item: (-item.uncovered_lines, item.path),
    )
    if not rows:
        return "No uncovered lines reported in coverage/lcov.info."
    output = ["| File | Uncovered lines |", "|---|---:|"]
    output.extend(
        f"| `{item.path}` | {item.uncovered_lines} |" for item in rows[:limit]
    )
    return "\n".join(output)


def parse_args(arguments: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("hotspots",))
    parser.add_argument("--root", type=Path)
    parser.add_argument("--lcov", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=15)
    return parser.parse_args(arguments)


def main(arguments: list[str] | None = None) -> int:
    args = parse_args(arguments)
    try:
        root = args.root.resolve() if args.root else repository_root(Path(__file__).parent)
        stats = parse_lcov(root, args.lcov)
        print(hotspot_markdown(stats, limit=args.limit))
    except WorkflowError as error:
        print(f"lcov-stats: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
