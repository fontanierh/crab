from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.lcov_stats import hotspot_markdown, parse_lcov
from scripts.tests.helpers import init_repo, write
from scripts.workflow_common import WorkflowError


def record(root: Path, body: str) -> str:
    return f"SF:{root / 'crates/alpha/src/lib.rs'}\n{body}end_of_record\n"


class LcovStatsTests(unittest.TestCase):
    def test_lf_lh_remain_truthful_when_zero_hit_da_rows_are_absent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            lcov = root / "coverage.info"
            write(lcov, record(root, "DA:1,3\nDA:2,1\nLF:10\nLH:8\n"))
            stats = parse_lcov(root, lcov)
        item = stats.files[0]
        self.assertEqual((item.lines_found, item.lines_hit, item.uncovered_lines), (10, 8, 2))
        self.assertEqual(item.zero_hit_lines, ())
        self.assertFalse(item.locations_complete)
        self.assertIn("| `crates/alpha/src/lib.rs` | 2 |", hotspot_markdown(stats))

    def test_complete_da_rows_agree_with_lf_lh(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            lcov = root / "coverage.info"
            write(lcov, record(root, "DA:1,4\nDA:2,0\nLF:2\nLH:1\n"))
            item = parse_lcov(root, lcov).files[0]
        self.assertEqual(item.zero_hit_lines, (2,))
        self.assertTrue(item.locations_complete)
        self.assertFalse(item.totals_from_da)

    def test_positive_da_rows_cannot_exceed_lh(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            lcov = root / "coverage.info"
            write(
                lcov,
                record(root, "DA:1,1\nDA:2,1\nDA:3,1\nDA:4,1\nLF:5\nLH:3\n"),
            )
            with self.assertRaisesRegex(WorkflowError, "positive DA rows exceed LH"):
                parse_lcov(root, lcov)

    def test_positive_da_row_cannot_contradict_zero_lh(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            lcov = root / "coverage.info"
            write(lcov, record(root, "DA:1,1\nLF:1\nLH:0\n"))
            with self.assertRaisesRegex(WorkflowError, "DA rows disagree"):
                parse_lcov(root, lcov)

    def test_complete_da_universe_must_exactly_match_lf_lh(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            lcov = root / "coverage.info"
            write(lcov, record(root, "DA:1,1\nDA:2,1\nLF:2\nLH:1\n"))
            with self.assertRaisesRegex(WorkflowError, "DA rows disagree"):
                parse_lcov(root, lcov)

    def test_contradictory_duplicate_record_is_not_laundered(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            lcov = root / "coverage.info"
            write(
                lcov,
                record(root, "DA:1,0\nLF:1\nLH:0\n")
                + record(root, "DA:1,1\nLF:1\nLH:0\n"),
            )
            with self.assertRaisesRegex(WorkflowError, "DA rows disagree"):
                parse_lcov(root, lcov)

    def test_duplicate_records_merge_hits_per_line_instead_of_summing_totals(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            lcov = root / "coverage.info"
            write(
                lcov,
                record(root, "DA:1,0\nLF:1\nLH:0\n")
                + record(root, "DA:1,2\nLF:1\nLH:1\n"),
            )
            stats = parse_lcov(root, lcov)
        self.assertEqual((stats.lines_found, stats.lines_hit, stats.uncovered_lines), (1, 1, 0))

    def test_duplicate_record_with_incomplete_da_universe_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            lcov = root / "coverage.info"
            write(
                lcov,
                record(root, "DA:1,1\nLF:2\nLH:1\n")
                + record(root, "DA:2,1\nLF:1\nLH:1\n"),
            )
            with self.assertRaisesRegex(WorkflowError, "cannot be merged"):
                parse_lcov(root, lcov)

    def test_malformed_totals_and_da_fields_fail_closed(self) -> None:
        cases = {
            "lh-over-lf": "DA:1,1\nLF:1\nLH:2\n",
            "bad-lf": "LF:nope\nLH:0\n",
            "negative-hit": "DA:1,-1\n",
            "missing-lh": "LF:1\n",
            "inconsistent-complete-da": "DA:1,0\nLF:1\nLH:1\n",
        }
        for name, body in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                init_repo(root)
                lcov = root / "coverage.info"
                write(lcov, record(root, body))
                with self.assertRaises(WorkflowError):
                    parse_lcov(root, lcov)


if __name__ == "__main__":
    unittest.main()
