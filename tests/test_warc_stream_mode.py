from io import BytesIO

from warcio.archiveiterator import ArchiveIterator

from src.load_from_db import units_from_warc

from .test_load_from_db_warc import make_shard


def test_stream_single_shard_yields_units(tmp_path):
    cases = [({"ID": f"c{i}"}, f"<html>case {i}</html>".encode()) for i in range(3)]
    make_shard(tmp_path / "s1.warc.gz", cases)

    units = list(units_from_warc(str(tmp_path / "s1.warc.gz")))

    assert len(units) == 3
    assert [u["meta"]["ID"] for u in units] == ["c0", "c1", "c2"]
    for i, u in enumerate(units):
        [fname] = u["html_files"]
        assert u["html_bytes_by_name"][fname] == f"<html>case {i}</html>".encode()


def test_stream_locator_is_seekable(tmp_path):
    cases = [({"ID": f"c{i}"}, f"<html>case {i}</html>".encode()) for i in range(3)]
    make_shard(tmp_path / "s1.warc.gz", cases)

    unit = list(units_from_warc(str(tmp_path / "s1.warc.gz")))[1]

    # locator must read back the same group via plain seek, like db mode
    path, off, length = unit["manifest_path"].rsplit(":", 2)
    with open(path, "rb") as fh:
        fh.seek(int(off))
        buf = BytesIO(fh.read(int(length)))
    recs = [(r.rec_type, r.content_stream().read()) for r in ArchiveIterator(buf)]
    assert {t for t, _ in recs} == {"metadata", "resource"}
    assert next(b for t, b in recs if t == "resource") == b"<html>case 1</html>"


def test_stream_directory_of_shards(tmp_path):
    make_shard(tmp_path / "a.warc.gz", [({"ID": "a0"}, b"<html>a</html>")])
    make_shard(tmp_path / "b.warc.gz", [({"ID": "b0"}, b"<html>b</html>")])
    (tmp_path / "noise.txt").write_text("ignore me")

    units = list(units_from_warc(str(tmp_path)))

    assert sorted(u["meta"]["ID"] for u in units) == ["a0", "b0"]


def test_stream_finds_shards_nested_one_level_down(tmp_path):
    """The MGS crawler used to nest each shard under its container id, so a
    reindex over the warc root saw nothing at all — every shard was one level
    below the directory being listed. Those trees still hold live data."""
    (tmp_path / "2c06d9e460a7").mkdir()
    (tmp_path / "b3a8d1d7cad1").mkdir()
    make_shard(tmp_path / "2c06d9e460a7" / "a.warc.gz",
               [({"ID": "nested-a"}, b"<html>a</html>")])
    make_shard(tmp_path / "b3a8d1d7cad1" / "b.warc.gz",
               [({"ID": "nested-b"}, b"<html>b</html>")])
    make_shard(tmp_path / "flat.warc.gz", [({"ID": "flat"}, b"<html>f</html>")])

    units = list(units_from_warc(str(tmp_path)))

    assert sorted(u["meta"]["ID"] for u in units) == [
        "flat", "nested-a", "nested-b"]
