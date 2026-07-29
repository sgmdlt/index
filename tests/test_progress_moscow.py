import json
from pathlib import Path

from src.bulk_indexer import merge_progress
from src.prepare_doc import prepare_doc


def _raw():
    p = Path(__file__).parent / "progress_test" / "moscow_movement.json"
    return json.loads(p.read_text(encoding="utf-8"))


def test_moscow_progress_flattens_states_and_hearings():
    doc = prepare_doc(_raw())
    prog = doc.get("progress") or []
    events = [r.get("event") for r in prog]

    # No degenerate empty rows.
    assert all(r.get("date") and r.get("event") for r in prog), prog

    # История состояний rows are flattened (date=Дата, event=Состояние).
    assert "Зарегистрировано" in events
    assert "Завершено" in events

    # Судебные заседания и беседы are included (event=Стадия).
    assert "Судебное заседание" in events

    # История местонахождения is NOT turned into progress rows.
    assert "У судьи" not in events

    # Exactly the 2 states + 1 hearing.
    assert len(prog) == 3, prog


def test_merge_drops_stale_empty_head_row():
    # A degenerate row (empty date+event) left in ES by the old parser must be
    # purged on re-index, not preserved forever by the merge.
    stale = [{"date": "", "event": "", "История местонахождения": [{"Дата": "08.08.2025"}]}]
    incoming = [
        {"date": "08.08.2025", "event": "Зарегистрировано"},
        {"date": "20.08.2025", "event": "Завершено"},
    ]
    merged = merge_progress(stale, incoming)
    assert all(r.get("date") and r.get("event") for r in merged), merged
    assert len(merged) == 2, merged
