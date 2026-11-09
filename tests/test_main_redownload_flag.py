from src.main import parse_args


def test_redownload_on_by_default():
    assert parse_args(["--index", "x", "--root", "r"]).redownload is True


def test_no_redownload_turns_it_off():
    args = parse_args(["--index", "x", "--root", "r", "--no-redownload"])
    assert args.redownload is False
