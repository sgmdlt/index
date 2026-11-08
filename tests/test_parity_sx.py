"""Parity test: src.parse_html (bs4) vs src.parse_html_sx (selectolax).

Runs both implementations over every HTML fixture in tests/*_test/ and asserts
identical raw_tables output. Also prints per-fixture timing for a quick
speedup signal.
"""

import time
from pathlib import Path

import pytest

from src.parse_html import fill_raw_tables as fill_bs
from src.parse_html_sx import fill_raw_tables as fill_sx


FIXTURE_ROOT = Path(__file__).parent
HTML_FIXTURES = sorted(FIXTURE_ROOT.glob("*_test/*.html"))


def _normalize(x):
    """Strip leading/trailing whitespace from every string leaf so parity
    isn't blocked by HTML5 (lexbor) foster-parenting moving whitespace
    around table boundaries.
    """
    if isinstance(x, str):
        return x.strip()
    if isinstance(x, dict):
        return {k: _normalize(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_normalize(v) for v in x]
    return x


@pytest.mark.parametrize("fixture", HTML_FIXTURES, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_parity(fixture: Path):
    html = fixture.read_text(encoding="utf-8")
    bs_out = _normalize(fill_bs(html))
    sx_out = _normalize(fill_sx(html))
    assert bs_out == sx_out, (
        f"Mismatch on {fixture.name}\n"
        f"bs keys: {sorted(bs_out.get('raw_tables', {}).keys())}\n"
        f"sx keys: {sorted(sx_out.get('raw_tables', {}).keys())}"
    )


def test_speed_summary(capsys):
    """Not an assertion — prints aggregate parse time for both impls."""
    if not HTML_FIXTURES:
        pytest.skip("no fixtures")

    htmls = [p.read_text(encoding="utf-8") for p in HTML_FIXTURES]

    t0 = time.perf_counter()
    for _ in range(5):
        for h in htmls:
            fill_bs(h)
    bs_ms = (time.perf_counter() - t0) * 1000

    t0 = time.perf_counter()
    for _ in range(5):
        for h in htmls:
            fill_sx(h)
    sx_ms = (time.perf_counter() - t0) * 1000

    with capsys.disabled():
        print(
            f"\n[parity-bench] fixtures={len(htmls)} runs=5 "
            f"bs={bs_ms:.1f}ms sx={sx_ms:.1f}ms speedup={bs_ms/sx_ms:.2f}x"
        )
