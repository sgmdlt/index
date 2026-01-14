import { Pool } from "pg";
import os from "os";
import fsp from "fs/promises";
import crypto from "crypto";
import minimist from "minimist";
import { parseFiles} from "./parser.js";
import path from "path";
import { Client } from "@elastic/elasticsearch";

import { toDocument } from "./toDocument.js";

const args = minimist(process.argv.slice(2), {
  string: ["index", "root", "es", "user", "pass", "apiKey", "tag"],
  boolean: ["insecure"],
  default: {
    es: "http://localhost:9222",
  },
});

const ES_URL = String(args.es);
const scriptId = 'painless';
const INDEX = 'sud'
const ROOT = path.resolve(String(args.root));
const TAG = args.tag ? String(args.tag) : null;

const esOpts = { node: ES_URL, requestTimeout: 120000 };
if (args.apiKey) esOpts.auth = { apiKey: String(args.apiKey) };
if (args.user && args.pass) {
  esOpts.auth = { username: String(args.user), password: String(args.pass) };
}
if (ES_URL.startsWith("https://") && args.insecure) {
  esOpts.tls = { rejectUnauthorized: false };
}
const es = new Client(esOpts);

const PG = {
  host: process.env.PGHOST || '127.0.0.1',
  port: Number(process.env.PGPORT || 8432),
  database: process.env.PGDATABASE || 'postgres',
  user: process.env.PGUSER || 'postgres',
  password: process.env.PGPASSWORD || 'celgthtytcen',
  max: Number(process.env.PGPOOL_MAX || 5),
};

const TABLE = 'outbox';
const BATCH_LIMIT = Number(process.env.BATCH_LIMIT || 500);
const LEASE_MINUTES = Number(process.env.LEASE_MINUTES || 5);
const MAX_ATTEMPTS = Number(process.env.MAX_ATTEMPTS || 5);

const pool = new Pool(PG);

const workerId =
  process.env.WORKER_ID ||
  `indexer@${os.hostname()}:${process.pid}:${crypto.randomUUID()}`;

function normalizeError(err) {
  if (!err) return 'Unknown error';
  if (typeof err === 'string') return err.slice(0, 4000);
  const msg = err.stack || err.message || JSON.stringify(err);
  return String(msg).slice(0, 4000);
}

async function getBatch(client, limit) {
    const sql = `
    WITH picked AS (
        SELECT id
        FROM outbox
        WHERE status = 'NEW'
           OR (status = 'IN_PROGRESS' AND lease_until < now())  -- опционально
        ORDER BY id
        FOR UPDATE SKIP LOCKED
        LIMIT $2
    )
        UPDATE outbox o
        SET status     = 'IN_PROGRESS',
            locked_at  = now(),
            locked_by  = $1,
            lease_until = now() + ($3)::interval,          -- опционально
            attempts   = o.attempts + 1
        FROM picked
        WHERE o.id = picked.id
        RETURNING o.id, o.manifest, o.filename, o.attempts`;
    const res = await client.query(sql, [workerId, limit, `${LEASE_MINUTES} minutes`]);
  return res.rows;
}

async function markDone(client, ids) {
  if (ids.length === 0) return;
  const sql = `
    UPDATE outbox
    SET status = 'DONE',
        last_error = NULL,
        locked_by = NULL,
        locked_at = NULL,
        lease_until = NULL
    WHERE id = ANY($1::bigint[]);
    `;
  await client.query(sql, [ids]);
}

async function markRetryOrFailed(client, rowId, attempts, errText) {
  const retry = attempts < MAX_ATTEMPTS;

  const sql = retry
    ? `
    UPDATE outbox
    SET status = 'NEW',
        last_error = $2,
        locked_by = NULL,
        locked_at = NULL,
        lease_until = NULL
    WHERE id = $1;
    `
    : `
    UPDATE outbox
    SET status = 'FAILED',
        last_error = $2,
        locked_by = NULL,
        locked_at = NULL,
        lease_until = NULL
    WHERE id = $1;
    `;

  await client.query(sql, [rowId, errText]);
}

async function readJsonSafe(file) {
  try {
    return JSON.parse(await fsp.readFile(file, "utf8"));
  } catch (e) {
    console.warn(`Пропуск (невалидный JSON): ${file}: ${e.message}`);
    return null;
  }
}

async function* makeDatasource(rows) {
  for (const r of rows) {
      r.manifest = r.manifest.replace("/app/", "/home/ltdmgs/courts/");
      const relFromRoot = r.filename.replace("/app/", "/home/ltdmgs/courts/");
      r.filename = r.filename.split('/').at(-1);
      const meta = await readJsonSafe(r.manifest);

      let unit = {
        manifestPath: r.manifest,
        htmlFiles: [r.filename],
        relFromRoot,
        meta
    }

      const parsedFiles = await parseFiles(unit)
      const doc = toDocument({...unit, parsedFiles})

      const docData = doc.files[0]
      const docId =
      doc.group_id ??
      doc.id_final ??
      doc.uuid ??
      doc.id_uid ??
      makeId(doc);

    const version = doc.version ?? (typeof r.version === "number" ? r.version : 0);
    const versionTs = doc.version_ts ?? r.version_ts ?? new Date().toISOString();

    function toArray(v) {
      if (v == null) return [];
      if (Array.isArray(v)) {
      return v.filter(x => x != null && !(typeof x === "string" && x.length === 0));
      }
      if (typeof v === "string" && v.length === 0) return [];
      return [v];
    }

    // Что добавляем “в накопительные поля” (как additions)
    const params = {
    version,
    version_ts: typeof versionTs === "string" ? versionTs : new Date(versionTs).toISOString(),

    defendants_add: toArray(docData.defendants),
    participants_add: toArray(docData.participants),
    participants_and_defendants_add: toArray(docData.participants_and_defendants),
    articles_add: toArray(docData.articles),
    full_document_texts_add: toArray(docData.full_document_texts),

    judge_latest: docData.judge ?? null,
    doc_text_full_latest: docData.doc_text_full ?? null,

    file_append: {
      relFromRoot: unit.relFromRoot,
      filename: r.filename,
      version_ts: typeof versionTs === "string" ? versionTs : new Date(versionTs).toISOString(),
    },
  };

    // Минимальный upsert, чтобы скрипт мог безопасно делать ctx._source.files.add(...)
    const upsert = {
      group_id: doc.group_id ?? docId,
      version: version,
      version_ts: versionTs,
      is_latest: false,
      manifestPath: unit.manifestPath,
      files: [],
      articles: [],
      defendants: [],
      participants: [],
      participants_and_defendants: [],
      full_document_texts: [],
      meta: doc.meta ?? unit.meta ?? {},
    };

    yield { _id: String(docId), params, upsert, outbox_id: r.id, attempts: r.attempts };
  }
}

async function ensureIndex() {
  try {
    const mappingPath = path.join(__dirname, "mapping_new.json");
    const mappingRaw = await fsp.readFile(mappingPath, "utf8");
    const mapping = JSON.parse(mappingRaw);
    await es.indices.create({ index: INDEX, body: mapping }, { ignore: [400] });
  } catch (e) {
    await es.indices.create({ index: INDEX }, { ignore: [400] });
  }
}

async function main() {
  await ensureIndex();
  const client = await pool.connect();
  try {
    // 1) Claim пачки — короткая транзакция
    await client.query("BEGIN");
    const rows = await getBatch(client, BATCH_LIMIT);
    await client.query("COMMIT");

    if (rows.length === 0) {
      console.log(`[${new Date().toISOString()}] Nothing to index. worker=${workerId}`);
      return;
    }

    console.log(
      `[${new Date().toISOString()}] Claimed ${rows.length} rows. worker=${workerId}`
    );

    const failed = [];

    // 2) Bulk update в ES (Painless script)
    const operations = [];
    const refByDocId = new Map(); // docId -> { outboxId, attempts }

  for await (const item of makeDatasource(rows)) {
  // связываем ответ ES с outbox-строкой
    refByDocId.set(String(item._id), { outboxId: item.outbox_id, attempts: item.attempts });

    operations.push({ update: { _index: INDEX, _id: String(item._id) } });
    operations.push({
      scripted_upsert: true,
      script: { lang: "painless", source: await fsp.readFile("/home/ltdmgs/index/merge_case_files_v1", "utf8"), params: item.params },
      upsert: item.upsert,
  });
}

let bulkResp;
try {
  bulkResp = await es.bulk({ refresh: false, operations });
} catch (e) {
  const errText = normalizeError(e);
  console.error(`Bulk request failed: ${errText}`);

  // если упал весь bulk-запрос — откатываем пачку в retry/failed
  for (const row of rows) {
    await markRetryOrFailed(client, row.id, row.attempts, errText);
  }
  return;
}

// разбор результата bulk
const items = bulkResp.items || (bulkResp.body && bulkResp.body.items) || [];
console.log("ITEMS = ", items[0].update.error);
const failedIds = new Set();
const doneIds = [];

for (const it of items) {
  const upd = it.update;
  if (!upd) continue;

  const docId = String(upd._id);
  const ref = refByDocId.get(docId);
  if (!ref) continue;

  if (upd.error) {
    failedIds.add(ref.outboxId);
    const errText = JSON.stringify(upd.error).slice(0, 4000);
    await markRetryOrFailed(client, ref.outboxId, ref.attempts, errText);
  }
}

// всё, что не в failed — DONE
for (const row of rows) {
  if (!failedIds.has(row.id)) doneIds.push(row.id);
}

await markDone(client, doneIds);

    console.log(
      `[${new Date().toISOString()}] Done=${doneIds.length}, Failed=${failed.length}, Total=${rows.length}`
    );
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (_) {}
    console.error(normalizeError(err));
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((e) => {
  console.error(normalizeError(e));
  process.exitCode = 1;
});
