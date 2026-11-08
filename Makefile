up:
	docker compose up -d

remake: delete_index run refresh_index

delete_index:
	curl -XDELETE http://localhost:9222/sud	|| true

run:
	node main.js --index sud --root ./court_cases

refresh_index:
	curl -XPOST http://localhost:9222/sud/_refresh

CONTAINER_NAME=test_postgres
IMAGE_NAME=test_postgres

test_db:
	docker rm -f $(CONTAINER_NAME) || true
	docker build -t $(IMAGE_NAME) -f Dockerfile.test .

init_db: test_db
	docker run -d \
		--name $(CONTAINER_NAME) \
		-p 8433:5432 \
		$(IMAGE_NAME)

retry_db: delete_index init_db
	node loadFromDb.js --root ./court_cases --index sud

test:
	uv run pytest -vsrP

MINIO_USER ?= $(or $(MINIO_ROOT_USER),minioadmin)
MINIO_PASS ?= $(or $(MINIO_ROOT_PASSWORD),minioadmin)

minio_init:
	docker run --rm --network=host \
		-e MC_HOST_local=http://$(MINIO_USER):$(MINIO_PASS)@localhost:9100 \
		minio/mc mb -p local/courts

upload_2026:
	uv run python scripts/upload_to_minio.py

# Index a gzipped JSONL file readable only via sudo.
# Usage:
#   make index_gz FILE=/home/ikuzminkin/sud_delo/.../foo.jsonl.gz INDEX=test_stop_words
# Optional override: ES_USER=..., SGMDLT_PASSWORD=...
index_gz:
	@test -n "$(FILE)"  || { echo "FILE required (path to .jsonl.gz)"; exit 1; }
	@test -n "$(INDEX)" || { echo "INDEX required"; exit 1; }
	set -a; [ -f .env ] && . ./.env; set +a; \
	sudo zcat $(FILE) | uv run python -m src.main \
		--index $(INDEX) \
		--user $${ES_USER} \
		--pass $${SGMDLT_PASSWORD} \
		--jsonl -
