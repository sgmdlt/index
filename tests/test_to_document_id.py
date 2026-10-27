from src.main import _derive_id_final
from src.utils.case_id import case_id_from_fields
from src.to_document import to_document

_REL = "2016/03/30/RS/61RS0018/УК/61RS0018_2016-03-30_1-76/manifest.json"


def _meta(**kw):
    base = {
        "case_number": "1-76/2016",
        "date_reg": "30.03.2016",
        "court_id": "61RS0018",
        "ID": "61RS0018_WRONGLEGACY_1-76",  # stale/wrong stored id
    }
    base.update(kw)
    return base


def test_id_recomputed_over_stale_manifest_id():
    doc = to_document("m.json", _REL, _meta(), [], [])
    assert doc["id_final"] == "61RS0018_2016-03-30_1-76"


def test_old_and_new_collision_converge_to_same_id():
    # two scrapes of one case under different stored ids -> same recomputed _id
    old = to_document("m.json", _REL, _meta(ID="61RS0018_NONE_1-76"), [], [])
    new = to_document("m.json", _REL, _meta(ID="61RS0018_2016-03-30_1-76"), [], [])
    assert old["id_final"] == new["id_final"] == "61RS0018_2016-03-30_1-76"


def test_fallback_to_stored_id_when_no_date_reg():
    doc = to_document("m.json", _REL, _meta(date_reg=None, ID="61RS0018_2016-03-30_1-76"), [], [])
    assert doc["id_final"] == "61RS0018_2016-03-30_1-76"


def test_hmao_region_alias_folds_to_one_name():
    meta = _meta(region="Ханты-Мансийский автономный округ - Югра (Тюменская область)")
    doc = to_document("m.json", _REL, meta, [], [])
    assert doc["meta"]["region"] == "Ханты-Мансийский автономный округ - Югра"


def test_jsonl_fields_recompute_over_stale_id_final():
    # JSONL несёт court_code и ISO-дату, а id_final посчитан старой формулой
    meta = {"case_number": "2-1/2011 (2-2/2010;)", "date_reg": "2008-12-09T00:00:00Z",
            "court_code": "78OS0000", "id_final": "78OS0000_2008-12-09_2-1"}
    assert to_document("", "x", meta, [], [])["id_final"] == "78OS0000_2008-12-09_2-2"


def test_iso_and_ru_dates_give_one_id():
    assert case_id_from_fields("1-76/2016", "2016-03-30", "61RS0018", "") == \
           case_id_from_fields("1-76/2016", "30.03.2016", "61RS0018", "") == "61RS0018_2016-03-30_1-76"


def test_mgs_keeps_its_own_id():
    # у МГС свой ключ (parse_html_mgs.card_case_id), формула sudrf его не трогает
    meta = {"case_number": "02-3415/2025 ∼ М-9183/2024", "date_reg": "2025-01-14", "court_code": "77RS0018",
            "link": "https://mos-gorsud.ru/rs/x/services/cases/civil/details/abc",
            "id_final": "77RS0018_2025-01-14_02-3415"}
    assert to_document("", "x", meta, [], [])["id_final"] == "77RS0018_2025-01-14_02-3415"


def test_derive_id_final_recomputes_jsonl_record():
    data = {"case_number": "2-1/2011 (2-2/2010;)", "date_reg": "2008-12-09",
            "court_code": "78OS0000", "id_final": "78OS0000_2008-12-09_2-1"}
    assert _derive_id_final(data) == "78OS0000_2008-12-09_2-2"
