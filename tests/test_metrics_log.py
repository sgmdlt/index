from src.metrics import Metrics


def test_log_line_shows_refresh_time(capsys):
    # refresh перед поиском близнецов — главная новая цена записи (courts-2ymt.2)
    metrics = Metrics()
    metrics.add_time("refreshMs", 1234.0)
    metrics.log(force=True)
    assert "refresh_ms=1234" in capsys.readouterr().out
