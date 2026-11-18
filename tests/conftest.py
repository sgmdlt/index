"""Настоящий тестовый Elasticsearch для тестов, которым мало заглушки.

Адрес — TEST_ES_URL (mise ставит его для `mise run check`, локально ES
поднимает `docker compose --profile test up -d --wait es-test`). Без ES тест
падает, а не пропускается: проверка качества одинакова локально и в CI.
Писать можно только в кластер с именем index-test — на этом хосте рядом
работает продовый ES, и опечатка в адресе не должна до него дотянуться.
"""

import json
import os
import uuid
from pathlib import Path

import pytest
from elasticsearch import Elasticsearch

TEST_CLUSTER = "index-test"
MAPPING = Path(__file__).parent.parent / "src" / "mapping_new.json"


@pytest.fixture(scope="session")
def test_es() -> Elasticsearch:
    url = os.environ.get("TEST_ES_URL")
    if not url:
        pytest.fail(
            "TEST_ES_URL не задан: запустите через `mise run check` или задайте "
            "его после `docker compose --profile test up -d --wait es-test`"
        )
    es = Elasticsearch(url, request_timeout=30)
    name = es.info()["cluster_name"]
    if name != TEST_CLUSTER:
        pytest.fail(
            f"{url} — кластер {name!r}, а писать можно только в {TEST_CLUSTER!r}"
        )
    return es


@pytest.fixture
def es_index(test_es: Elasticsearch):
    """Пустой индекс по продовому маппингу, refresh только вручную — как в
    прогоне индексатора (_fast_ingest_on)."""
    name = f"t-{uuid.uuid4().hex[:12]}"
    mapping = json.loads(MAPPING.read_text(encoding="utf-8"))
    test_es.indices.create(index=name, **mapping)
    test_es.indices.put_settings(
        index=name, settings={"index": {"refresh_interval": "-1"}}
    )
    yield name
    test_es.indices.delete(index=name)
