import { traverseCases } from "./traverse.js";
import { parseFiles } from "./parser.js";
import path from "path";
import minimist from "minimist";
import { toDocument } from "./toDocument.js";
import { Client } from "@elastic/elasticsearch";
import asyncLib from "async";
import { createMetrics } from "./metrics.js";


const args = minimist(process.argv.slice(2), {
  string: ["index", "root", "es", "user", "pass", "apiKey", "tag"],
  boolean: ["insecure"],
  default: {
    es: "http://localhost:9222",
  },
});

function req(name) {
  if (!args[name]) {
    console.error(`Отсутствует обязательный аргумент --${name}`);
    process.exit(2);
  }
}

req("index");
req("root");

const ES_URL = String(args.es);
const INDEX = String(args.index);
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

function idFn(d) {
  return d.id_final || `${d.group_id}__${d.version}`;
}

function bucketKey(unit) {
  const parts = unit.relFromRoot.split("/");
  return parts.slice(0, 2).join("/"); // YYYY/MM/DD
}

export async function bulkIndex(esClient, index, docs, makeId, { refresh = false } = {}) {
  const BATCH = 2000;
  const RETRIES = 3;

  let failedTotal = 0;

  for (let i = 0; i < docs.length; i += BATCH) {
    const part = docs.slice(i, i + BATCH);

    for (let attempt = 0; attempt <= RETRIES; attempt++) {
      try {
        const res = await esClient.helpers.bulk({
          datasource: part,
          onDocument(doc) {
            return { index: { _index: index, _id: makeId(doc) } };
          },
          refreshOnCompletion: refresh,
        });

        if (res.failed) {
          failedTotal += res.failed;
          console.warn(`Bulk: part ${i}/${docs.length} — failed=${res.failed}`);
        }

        break;
      } catch (e) {
        const status = e?.meta?.statusCode || 0;
        if ((status === 429 || status === 503) && attempt < RETRIES) {
          const delay = 500 * Math.pow(2, attempt);
          await new Promise((r) => setTimeout(r, delay));
          continue;
        }
        throw e;
      }
    }
  }

  return { failed: failedTotal, total: docs.length };
}


function createBucketIndexer({
  es,
  index,
  idFn,
  bulkIndex,
  metrics,
  bulkDocs = 2000,
  bulkFlushMs = 5000,
}) {
  const buffers = new Map(); // bkey -> docs[]
  const timers = new Map();  // bkey -> timeoutId

  let flushing = Promise.resolve();

  function getBuf(bkey) {
    let buf = buffers.get(bkey);
    if (!buf) {
      buf = [];
      buffers.set(bkey, buf);
    }
    return buf;
  }

  async function flushBucket(bkey) {
    const buf = buffers.get(bkey);
    if (!buf || buf.length === 0) return;

    buffers.set(bkey, []); // новый пустой буфер под этот бакет

    const batch = buf;

    const t0 = metrics.now();
    try {
      const res = await bulkIndex(es, index, batch, idFn, { refresh: false });
      metrics.inc("bulkCalls", 1);
      metrics.inc("docsIndexed", res.total);
      metrics.inc("bulkDocsTotal", res.total);
      if (res.failed) metrics.inc("bulkFailedDocs", res.failed);
    } finally {
      metrics.addTime("bulkMs", metrics.now() - t0);
      metrics.log(false);
    }
  }

  function clearTimer(bkey) {
    const t = timers.get(bkey);
    if (t) {
      clearTimeout(t);
      timers.delete(bkey);
    }
  }

  function scheduleFlush(bkey) {
    if (timers.has(bkey)) return;

    const t = setTimeout(() => {
      timers.delete(bkey);
      flushing = flushing.then(() => flushBucket(bkey));
    }, bulkFlushMs);

    timers.set(bkey, t);
  }

  async function push(bkey, doc) {
    const buf = getBuf(bkey);
    buf.push(doc);

    if (buf.length >= bulkDocs) {
      clearTimer(bkey);
      flushing = flushing.then(() => flushBucket(bkey));
    } else {
      scheduleFlush(bkey);
    }
  }

  async function drain() {
    // остановить таймеры
    for (const [bkey, t] of timers.entries()) {
      clearTimeout(t);
      timers.delete(bkey);
    }

    // дождаться уже запланированных flush
    await flushing;

    // добить всё, что осталось
    const keys = Array.from(buffers.keys());
    for (const bkey of keys) {
      await flushBucket(bkey);
    }
  }

  return { push, drain };
}



function collapseFiles(files) {
  if (!Array.isArray(files) || files.length === 0) {
    return null;
  }

  const defendantsSet = new Set();
  const judgesSet = new Set();
  const participantsSet = new Set();
  const documentsSet = new Set();
  const articlesSet = new Set();

  let anyFile = undefined;

  for (const f of files) {
    if (!f) {
      continue;
    }

    // забрать первый встретившийся file/document_text (если они появятся)
    if (anyFile === undefined && f.file !== undefined) {
      anyFile = f.file;
    }
    if (typeof f.full_document_texts === "string" && f.full_document_texts.trim() !== "") {
      documentsSet.add(f.full_document_texts.trim());
    }

    if (typeof f.defendants === "string" && f.defendants.trim() !== "") {
      defendantsSet.add(f.defendants.trim());
    }

    if (typeof f.judge === "string" && f.judge.trim() !== "") {
      judgesSet.add(f.judge.trim());
    }

    if (typeof f.participants === "string" && f.participants.trim() !== "") {
        participantsSet.add(f.participants.trim());
      }

    if (typeof f.articles === "string" && f.articles.trim() !== "") {
        f.articles
            .split("\n")
            .map(s => s.trim())
            .filter(Boolean)
            .forEach(s => articlesSet.add(s));
    }

  }

  const participants_and_defendants = new Set(
  [defendantsSet, participantsSet]
    .flatMap(s => [...s])
    .flatMap(v => v.split("\n"))
    .map(s => s.trim())
    .filter(Boolean)
);


  return {
    file: anyFile,
    full_document_texts: Array.from(documentsSet),
    defendants: Array.from(defendantsSet),
    judge: Array.from(judgesSet),
    participants: Array.from(participantsSet),
    participants_and_defendants: Array.from(participants_and_defendants),
    articles: Array.from(articlesSet),
  };
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

async function withFastIngestSettings(fn) {
  const settingsResp = await es.indices.getSettings({ index: INDEX });
  const origRefresh =
    settingsResp?.[INDEX]?.settings?.index?.refresh_interval ?? "1s";
  const origReplicas =
    settingsResp?.[INDEX]?.settings?.index?.number_of_replicas ?? "1";

  await es.indices.putSettings({
    index: INDEX,
    body: { index: { refresh_interval: "-1", number_of_replicas: "0" } },
  });

  try {
    await fn();
  } finally {
    await es.indices.putSettings({
      index: INDEX,
      body: { index: { refresh_interval: null, number_of_replicas: null } },
    });
    await es.indices.refresh({ index: INDEX });
  }
}

async function main() {
  const metrics = createMetrics({ logEveryMs: 5000 });
  await ensureIndex();

  const caseConcurrency = Math.max(1, 8);
  const fileConcurrency = Math.max(1, 2);
  const bulkDocs = Math.max(1, 3000);
  const bulkFlushMs = Math.max(10, 5000);

  const indexer = createBucketIndexer({
  es,
  index: INDEX,
  idFn,
  bulkIndex,
  metrics,
  bulkDocs,
  bulkFlushMs,
});

  await withFastIngestSettings(async () => {
    const indexCargo = asyncLib.cargoQueue(async (docs) => {
      const t0 = metrics.now();
      try {
        const res = await bulkIndex(es, INDEX, docs, idFn, { refresh: false });
        metrics.inc("bulkCalls", 1);
        metrics.inc("docsIndexed", res.total);
        if (res.failed) metrics.inc("bulkFailedDocs", res.failed);
      } catch (e) {
        metrics.inc("indexErrors", 1);
        throw e;
      } finally {
        metrics.addTime("bulkMs", metrics.now() - t0);
        metrics.log(false);
      }
    }, bulkDocs);

    indexCargo.error((err) => {
      console.error("Index cargo error:", err);
    });

  const parseQueue = asyncLib.queue(async (unit) => {
      metrics.inc("unitsSeen", 1);
      metrics.inc("htmlFilesSeen", Array.isArray(unit.htmlFiles) ? unit.htmlFiles.length : 0);

      const bkey = bucketKey(unit);

      const tParse = metrics.now();
      const parsedFiles = await parseFiles(unit, { concurrency: fileConcurrency });
      metrics.addTime("parseMs", metrics.now() - tParse);

      const tDoc = metrics.now();
      const doc = toDocument({ ...unit, parsedFiles, tag: TAG });
      metrics.addTime("toDocMs", metrics.now() - tDoc);

      if (Array.isArray(doc.files)) {
        const collapsed = collapseFiles(doc.files);
        doc.files = collapsed ? [collapsed] : [];
      }

      metrics.inc("docsPrepared", 1);

      await indexer.push(bkey, doc);
          }, caseConcurrency);

    parseQueue.error((err) => {
      metrics.inc("parseErrors", 1);
      console.error("Parse queue error:", err);
    });

    const SOFT_LIMIT = caseConcurrency * 10;

    for await (const unit of traverseCases(ROOT)) {
      while (parseQueue.length() > SOFT_LIMIT) {
        await new Promise((r) => setTimeout(r, 10));
      }
      await parseQueue.pushAsync(unit);
    }

    await parseQueue.drain();
    await indexer.drain();

    metrics.log(true);
  });
}

main().catch((err) => {
  console.error("Фатальная ошибка:", err);
  process.exit(1);
});
