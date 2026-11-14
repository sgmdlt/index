"""index.log должен нести время: по нему ищут, когда случился эпизод (courts-r44l)."""

import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.main import completed_line

ROOT = Path(__file__).resolve().parent.parent


def test_log_record_starts_with_timestamp():
    probe = "import src.main, logging; logging.getLogger('probe').warning('hi')"
    res = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert re.match(
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{4} WARNING probe: hi$",
        res.stderr.strip(),
    ), res.stderr


def test_completed_line_carries_finish_time():
    now = datetime(2026, 11, 14, 8, 32, 3, tzinfo=timezone(timedelta(hours=1)))
    counters = {"unitsSeen": 3, "docsPrepared": 2, "docsIndexed": 2, "parseErrors": 1}

    line = completed_line("sudata_v2", counters, 121.34, now)

    assert line == (
        "=== 2026-11-14T08:32:03+0100 Indexation COMPLETED: index=sudata_v2 "
        "units=3 docsPrepared=2 docsIndexed=2 parseErrors=1 elapsed=121.3s ==="
    )
