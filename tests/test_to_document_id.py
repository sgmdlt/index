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
