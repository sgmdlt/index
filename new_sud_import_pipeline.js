import fsp from "fs/promises";
import path from "path";
import minimist from "minimist";
import { Client } from "@elastic/elasticsearch";
import { fileURLToPath } from "url";
import cliProgress from "cli-progress";
import fillRawTables from "./html_to_raw.js";
import prepareDoc from "./prepare_doc.js";

const progressBar = new cliProgress.SingleBar(
  {
    format: "Progress [{bar}] {percentage}% | {value}/{total} lines",
    hideCursor: true,
  },
  cliProgress.Presets.shades_classic
);

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

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

const INDEX = String(args.index);
const ROOT = path.resolve(String(args.root));
const ES_URL = String(args.es);
const TAG = args.tag ? String(args.tag) : null;

/** @type {import('@elastic/elasticsearch').ClientOptions} */
const esOpts = { node: ES_URL, requestTimeout: 120000 };
if (args.apiKey) esOpts.auth = { apiKey: String(args.apiKey) };
if (args.user && args.pass) {
  esOpts.auth = { username: String(args.user), password: String(args.pass) };
}
if (ES_URL.startsWith("https://") && args.insecure) {
  esOpts.tls = { rejectUnauthorized: false };
}
const es = new Client(esOpts);

async function isDir(p) {
  try {
    const st = await fsp.stat(p);
    return st.isDirectory();
  } catch {
    return false;
  }
}

function htmlNameToDate(name) {
  if (typeof name !== "string") return null;
  const match = name.match(/^(\d{2})-(\d{2})-(\d{4})\.(\d{2}):(\d{2}):(\d{2})/);
  if (!match) return null;
  const [, dd, mm, yyyy, hh, min, ss] = match;
  const iso = `${yyyy}-${mm}-${dd}T${hh}:${min}:${ss}Z`;
  const time = Date.parse(iso);
  return Number.isNaN(time) ? null : new Date(time);
}

function compareFileEntries(a, b) {
  const aTime = a?.fileDate instanceof Date ? a.fileDate.getTime() : null;
  const bTime = b?.fileDate instanceof Date ? b.fileDate.getTime() : null;
  if (aTime !== null && bTime !== null) {
    if (aTime !== bTime) return aTime - bTime;
  } else if (aTime !== null) {
    return -1;
  } else if (bTime !== null) {
    return 1;
  }
  const aName = typeof a?.file === "string" ? a.file : "";
  const bName = typeof b?.file === "string" ? b.file : "";
  return aName.localeCompare(bName);
}

function normalizeManifestDate(s) {
  if (typeof s !== "string") return s;
  const m = s.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
  if (m) return `${m[3]}-${m[2]}-${m[1]}`;
  return s;
}

async function* traverseCases(rootDir) {
  const stack = [rootDir];

  while (stack.length) {
    const current = stack.pop();
    let entries;

    try {
      entries = await fsp.readdir(current, { withFileTypes: true });
    } catch (e) {
      console.warn(`Пропуск каталога: ${current}: ${e.message}`);
      continue;
    }

    let manifestPath = null;
    const htmlEntries = [];

    for (const entry of entries) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(full);
        continue;
      }

      if (!entry.isFile()) continue;

      if (entry.name === "manifest.json") {
        manifestPath = full;
        continue;
      }

      if (entry.name.toLowerCase().endsWith(".html")) {
        htmlEntries.push({ file: entry.name, fullPath: full });
      }
    }

    if (!manifestPath) continue;

    let manifest;
    try {
      manifest = JSON.parse(await fsp.readFile(manifestPath, "utf8"));
    } catch (e) {
      console.warn(`Пропуск (невалидный JSON): ${manifestPath}: ${e.message}`);
      continue;
    }

    const rel = path.relative(rootDir, manifestPath);
    const preferredId = manifest.ID;

    const baseRaw = {
      case_number: manifest?.case_number,
      date_reg: normalizeManifestDate(manifest?.date_reg),
      link: manifest?.link,
      instance: manifest?.instance,
      case_type: manifest?.case_type,
      court_code: manifest?.court_id,
      court_name: manifest?.court_name,
      region: manifest?.region,
    };

    const files = [];
    for (const html of htmlEntries) {
      const fileDate = htmlNameToDate(html.file);

      try {
        const htmlRaw = await fsp.readFile(html.fullPath, "utf8");
        const tabs = (await fillRawTables(htmlRaw)) || {};
        files.push({ file: html.file, tabs, fileDate });
      } catch (e) {
        files.push({
          file: html.file,
          error: `read_or_parse_error: ${e.message}`,
          fileDate,
        });
      }
    }

    const sortedFiles = files
      .filter((f) => f && typeof f.file === "string")
      .sort(compareFileEntries);
    if (!sortedFiles.length) continue;

    const manifestPathNormalized = manifestPath.replace(/\\/g, "/");
    const relDirNormalized = path.dirname(rel).replace(/\\/g, "/");
    const orderedHtmlFiles = sortedFiles.map((f) => f.file);
    const latestIndex = sortedFiles.length - 1;

    for (let idx = 0; idx < sortedFiles.length; idx++) {
      const entry = sortedFiles[idx];
      const version = idx + 1;
      const versionTs =
        entry.fileDate instanceof Date ? entry.fileDate.toISOString() : null;
      const isLatest = idx === latestIndex;

      const raw = {
        ...baseRaw,
        type_instance: entry?.tabs?.type_instance,
        raw_tables: entry?.tabs?.raw_tables || {},
        document_text: entry?.docInfo?.document_text,
        path_manifest: manifestPathNormalized,
        rel_dir: relDirNormalized,
        html_file: entry.file,
        html_files: orderedHtmlFiles,
        version,
        version_ts: versionTs,
        is_latest: isLatest,
      };
      if (entry?.error) raw.file_error = entry.error;

      const metaPayload = {
        path_manifest: manifestPathNormalized,
        rel_dir: relDirNormalized,
        html_file: entry.file,
        html_files: orderedHtmlFiles,
        version,
        version_ts: versionTs,
      };
      if (entry?.error) metaPayload.error = entry.error;

      const fileInfo = {
        name: entry.file,
        collected_at: versionTs || undefined,
        error: entry?.error || undefined,
      };
      if (!fileInfo.collected_at) delete fileInfo.collected_at;
      if (!fileInfo.error) delete fileInfo.error;

      yield {
        group_id: preferredId,
        doc_id: `${preferredId}::${entry.file}`,
        version,
        version_ts: versionTs,
        is_latest: isLatest,
        raw,
        meta: metaPayload,
        files: [fileInfo],
        doc_text_full: entry?.docInfo?.document_text,
        tag: TAG,
      };
    }
  }
}

function toIndexedDocument(unit) {
  const doc = prepareDoc(unit.raw);
  doc.group_id = unit.group_id;
  doc.version = unit.version;
  if (unit.version_ts) doc.version_ts = unit.version_ts;
  doc.is_latest = unit.is_latest === true;
  if (unit.meta) doc.meta = unit.meta;
  if (unit.files?.length) doc.files = unit.files;
  if (unit.doc_text_full) doc.doc_text_full = unit.doc_text_full;
  if (unit.raw?.html_file) doc.html_file = unit.raw.html_file;
  if (unit.tag) doc.tag = unit.tag;

  const docId = unit.doc_id || `${unit.group_id}::${unit.version}`;
  Object.defineProperty(doc, "__docId", {
    value: docId,
    enumerable: false,
    configurable: true,
  });

  const manifestId =
    unit.meta?.path_manifest || unit.raw?.path_manifest || null;
  Object.defineProperty(doc, "__manifestId", {
    value: manifestId,
    enumerable: false,
    configurable: true,
  });

  return doc;
}

async function* preparedDocuments(rootDir) {
  for await (const unit of traverseCases(rootDir)) {
    yield toIndexedDocument(unit);
  }
}

async function main() {
  if (!(await isDir(ROOT))) {
    console.error(`--root должен указывать на каталог: ${ROOT}`);
    process.exit(2);
  }

  try {
    const mappingPath = path.join(__dirname, "mapping_new.json");
    const mappingRaw = await fsp.readFile(mappingPath, "utf8");
    const mapping = JSON.parse(mappingRaw);
    await es.indices.create({ index: INDEX, body: mapping }, { ignore: [400] });
  } catch (e) {
    await es.indices.create({ index: INDEX }, { ignore: [400] });
  }

  progressBar.start(1, 0);
  let processedLines = 0;
  const manifestTracker = new Set();

  const res = await es.helpers.bulk({
    datasource: preparedDocuments(ROOT),
    onDocument(doc) {
      processedLines++;
      const targetTotal = processedLines;
      if (progressBar.getTotal() <= targetTotal) {
        progressBar.setTotal(targetTotal);
      }
      progressBar.update(processedLines);

      if (doc.__manifestId && !manifestTracker.has(doc.__manifestId)) {
        manifestTracker.add(doc.__manifestId);
      }

      const docId =
        doc.__docId ||
        (doc.group_id ? `${doc.group_id}::${doc.version}` : undefined);

      const action = { index: { _index: INDEX } };
      if (docId) action.index._id = docId;

      delete doc.__docId;
      delete doc.__manifestId;

      return [action, doc];
    },
    onDrop(doc) {
      console.log(doc);
    },
  });

  progressBar.stop();

  console.log(
    JSON.stringify(
      {
        manifests_found: manifestTracker.size,
        indexed_successful: res.successful,
        indexed_failed: res.failed,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  progressBar.stop();
  console.error("Фатальная ошибка:", err);
  process.exit(1);
});
