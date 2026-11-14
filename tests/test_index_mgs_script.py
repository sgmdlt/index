"""index_mgs.sh не подставляет индекс сам: дефолт протух, когда sudata удалили (courts-r44l)."""

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "index_mgs.sh"


def _run(tmp_path, **env):
    # UV=echo: вместо индексатора печатаем, с какими аргументами его бы запустили
    base = {"PATH": os.environ["PATH"], "UV": "echo", "ROOT": str(tmp_path)}
    return subprocess.run(
        ["bash", str(SCRIPT)],
        env={**base, **env},
        capture_output=True,
        text=True,
    )


def test_refuses_to_run_without_index(tmp_path):
    res = _run(tmp_path)

    assert res.returncode != 0
    assert "INDEX" in res.stderr


def test_passes_index_through(tmp_path):
    res = _run(tmp_path, INDEX="sudata_v2")

    assert res.returncode == 0, res.stderr
    assert "--index sudata_v2" in res.stdout
