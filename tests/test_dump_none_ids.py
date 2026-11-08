from src.dump_none_ids import is_warc_locator


def test_warc_locator_detected():
    assert is_warc_locator("/app/court_warc/2026/shard-00012.warc:123456:7890")


def test_fs_path_not_locator():
    assert not is_warc_locator("/app/court_cases/26/MGS/.../manifest.json")


def test_windows_style_colon_not_locator():
    assert not is_warc_locator("2026/05/12/MGS/court/type/case/manifest.json")
