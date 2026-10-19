import pytest

from src.main import parse_args


def test_priority_only_flag_with_db():
    args = parse_args(["--index", "x", "--db", "--priority-only"])
    assert args.priority_only is True


def test_priority_only_defaults_false():
    assert parse_args(["--index", "x", "--db"]).priority_only is False


def test_priority_only_requires_db():
    with pytest.raises(SystemExit):
        parse_args(["--index", "x", "--priority-only"])
