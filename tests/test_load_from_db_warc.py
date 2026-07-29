import json
from io import BytesIO

from warcio.warcwriter import WARCWriter

from src.load_from_db import rows_to_units


def append_group(fh, writer, meta: dict, html: bytes) -> str:
    off = fh.tell()
    rec = writer.create_warc_record(
        "http://c/case",
        "metadata",
        payload=BytesIO(json.dumps(meta).encode()),
        warc_content_type="application/json",
    )
    writer.write_record(rec)
    rec = writer.create_warc_record(
        "http://c/case",
        "resource",
        payload=BytesIO(html),
        warc_content_type="text/html",
    )
    writer.write_record(rec)
    fh.flush()
    return f"{fh.name}:{off}:{fh.tell() - off}"


def make_shard(path, cases):
    locators = []
    with open(path, "ab") as fh:
        writer = WARCWriter(fh, gzip=True)
        for meta, html in cases:
            locators.append(append_group(fh, writer, meta, html))
    return locators


def row(i, locator):
    return {"id": i, "filename": locator, "manifest": locator,
            "attempts": 0, "warc_locator": locator}


def test_warc_rows_yield_units_without_fs_tree(tmp_path):
    meta = {"ID": "x_2025-01-01_1-1", "court_id": "krg"}
    html = b"<html>case body</html>"
    [locator] = make_shard(tmp_path / "s1.warc.gz", [(meta, html)])

    [(unit, out_row, reason)] = list(rows_to_units([row(1, locator)]))

    assert reason is None
    assert out_row["id"] == 1
    assert unit["meta"] == meta
    assert unit["manifest_path"] == locator
    # filename synthesized from WARC-Date (download time) so version_ts works
    [fname] = unit["html_files"]
    assert fname.endswith(".html")
    assert unit["html_bytes_by_name"] == {fname: html}


def test_warc_batch_two_shards_shuffled_order(tmp_path):
    cases_a = [({"ID": f"a{i}"}, f"<html>a{i}</html>".encode()) for i in range(3)]
    cases_b = [({"ID": f"b{i}"}, f"<html>b{i}</html>".encode()) for i in range(3)]
    locs_a = make_shard(tmp_path / "a.warc.gz", cases_a)
    locs_b = make_shard(tmp_path / "b.warc.gz", cases_b)

    shuffled = [row(1, locs_a[2]), row(2, locs_b[1]), row(3, locs_a[0]),
                row(4, locs_b[2]), row(5, locs_a[1]), row(6, locs_b[0])]
    results = list(rows_to_units(shuffled))

    assert len(results) == 6
    assert all(reason is None for _, _, reason in results)
    by_id = {r["id"]: u for u, r, _ in results}
    assert by_id[3]["meta"] == {"ID": "a0"}
    assert by_id[4]["meta"] == {"ID": "b2"}
    got_ids = [u["meta"]["ID"] for u, _, _ in results]
    # grouped per shard, ascending offsets within each shard
    assert got_ids in (["a0", "a1", "a2", "b0", "b1", "b2"],
                       ["b0", "b1", "b2", "a0", "a1", "a2"])


def test_warc_missing_shard_yields_path_missing(tmp_path):
    locator = f"{tmp_path}/gone.warc.gz:0:100"
    [(unit, _, reason)] = list(rows_to_units([row(1, locator)]))
    assert unit is None
    assert reason == "path_missing"


def test_legacy_row_without_locator_uses_fs(tmp_path):
    case_dir = tmp_path / "case"
    case_dir.mkdir()
    (case_dir / "manifest.json").write_text('{"ID": "legacy"}')
    (case_dir / "1.html").write_text("<html>legacy</html>")
    legacy = {"id": 9, "filename": str(case_dir / "1.html"),
              "manifest": str(case_dir / "manifest.json"),
              "attempts": 0, "warc_locator": None}

    [(unit, _, reason)] = list(rows_to_units([legacy]))

    assert reason is None
    assert unit["meta"] == {"ID": "legacy"}
    assert unit["html_files"] == ["1.html"]
    assert "html_bytes_by_name" not in unit


def test_shard_path_prefix_is_configurable(tmp_path, monkeypatch):
    """The crawler's container path is not always /app/court_warc/.

    The MGS crawler writes /app/out/<host>/<shard>.warc.gz, so the prefix to
    strip has to be settable alongside the host root it is replaced with.
    """
    import importlib

    import src.load_from_db as mod

    meta = {"ID": "x_2025-01-01_1-1"}
    [locator] = make_shard(tmp_path / "s1.warc.gz", [(meta, b"<html>c</html>")])
    shard, off, length = locator.rsplit(":", 2)
    container = f"/app/out/{shard.rsplit('/', 1)[1]}:{off}:{length}"

    monkeypatch.setenv("COURTS_WARC_DOCKER_ROOT", "/app/out/")
    monkeypatch.setenv("COURTS_WARC_ROOT", f"{tmp_path}/")
    mod = importlib.reload(mod)
    try:
        [(unit, _, reason)] = list(mod.rows_to_units([row(1, container)]))
        assert reason is None
        assert unit["meta"] == meta
    finally:
        monkeypatch.undo()
        importlib.reload(mod)
