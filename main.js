import { traverseCases } from "./traverse.js";
import { parseFiles } from "./parser.js";
import path from "path";
import minimist from "minimist";
import { toDocument } from "./toDocument.js";
import { Client } from "@elastic/elasticsearch";

const args = minimist(process.argv.slice(2), {
  string: ["index", "root", "es", "user", "pass", "apiKey", "tag"],
  boolean: ["insecure"],
  default: {
    es: "http://localhost:9200",
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

export async function resetIsLatest(es, index, groupIdSet) {
  const all = Array.from(groupIdSet);
  const chunkSize = 800; // запас под лимиты
  for (let i = 0; i < all.length; i += chunkSize) {
    const chunk = all.slice(i, i + chunkSize);
    await es.updateByQuery({
      index,
      conflicts: "proceed",
      refresh: true,
      body: {
        script: { source: "ctx._source.is_latest = false", lang: "painless" },
        query: { terms: { group_id: chunk } },
      },
    });
  }
}

export async function bulkIndex(es, index, docs, idFn) {
  const BATCH = 2000;
  const RETRIES = 3;

  for (let i = 0; i < docs.length; i += BATCH) {
    const part = docs.slice(i, i + BATCH);
    for (let attempt = 0; attempt <= RETRIES; attempt++) {
      try {
        const res = await es.helpers.bulk({
          datasource: part,
          onDocument(doc) {
            return { index: { _index: index, _id: idFn(doc) } };
          },
          refreshOnCompletion: true,
        });
        if (res.failed) {
          console.warn(`Bulk: part ${i}/${docs.length} — failed=${res.failed}`);
        }
        break; // успех — выходим из ретраев
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
}

const MAX_DOCS = 2000;
let buffer = [];
let groups = new Set();

async function flushBatch() {
  if (!buffer.length) return;
  await resetIsLatest(es, INDEX, groups);
  await bulkIndex(
    es,
    INDEX,
    buffer,
    (d) => d.id_final || `${d.group_id}__${d.version}`
  );
  buffer = [];
  groups = new Set();
}

let curBucket = null;
function bucketKey(unit) {
  const parts = unit.relFromRoot.split("/");
  return parts.slice(0, 3).join("/"); // YYYY/MM/DD
}

async function main() {
  try {
    const mappingPath = path.join(__dirname, "mapping_new.json");
    const mappingRaw = await fsp.readFile(mappingPath, "utf8");
    const mapping = JSON.parse(mappingRaw);
    await es.indices.create({ index: INDEX, body: mapping }, { ignore: [400] });
  } catch (e) {
    await es.indices.create({ index: INDEX }, { ignore: [400] });
  }

  for await (const unit of traverseCases(ROOT)) {
    const bkey = bucketKey(unit);
    if (curBucket !== null && bkey !== curBucket) {
      await flushBatch();
    }
    curBucket = bkey;

    const parsedFiles = await parseFiles(unit, { concurrency: 4 });
    // console.log(unit);
    const doc = toDocument({ ...unit, parsedFiles, tag: TAG });

    // локально внутри ведра можно отмечать latest по батчу,
    // но ключевое — перед bulk мы всё равно сбрасываем старые в индексе
    buffer.push(doc);
    console.log(doc);
    groups.add(doc.group_id);

    if (buffer.length >= MAX_DOCS) {
      await flushBatch();
    }
  }
  await flushBatch();
}

main().catch((err) => {
  console.error("Фатальная ошибка:", err);
  process.exit(1);
});
