import fsp from "fs/promises";
import path from "path";
import fillRawTables from "./html_to_raw.js";
import prepareDoc from "./prepare_doc.js";

async function mapLimit(items, limit = 4, fn) {
  const ret = new Array(items.length);
  let i = 0;

  async function worker() {
    while (true) {
      const idx = i++;
      if (idx >= items.length) return;
      ret[idx] = await fn(items[idx], idx);
    }
  }

  const workers = Array.from({ length: Math.min(limit, items.length) }, () =>
    worker()
  );
  await Promise.all(workers);
  return ret;
}

export async function parseFiles(unit, options = {}) {
  const { manifestPath, htmlFiles } = unit;
  const concurrency = Number.isFinite(options.concurrency)
    ? Math.max(1, options.concurrency)
    : 4;

  const dir = path.dirname(manifestPath);

  // Ничего парсить
  if (!htmlFiles || htmlFiles.length === 0) return [];

  return mapLimit(htmlFiles, concurrency, async (name) => {
    const fullPath = path.join(dir, name);
    try {
      const html = await fsp.readFile(fullPath, "utf8");
      const raw = (await fillRawTables(html)) || {};
      const parsed = await prepareDoc(raw);
      return parsed;
    } catch (e) {
      return {
        file: name,
        error: `read_or_parse_error: ${e?.message || String(e)}`,
      };
    }
  });
}
