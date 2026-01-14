import Piscina from "piscina";
import path from "path";
import { fileURLToPath } from "url";
import fsp from "fs/promises";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const piscina = new Piscina({
  filename: path.join(__dirname, "parse_worker.js"),
});

export async function parseFiles(unit, { concurrency = 2, metrics = null } = {}) {
  const { manifestPath, htmlFiles } = unit;
  const dir = path.dirname(manifestPath);
  const limit = Math.max(1, Number(concurrency) || 1);

  let i = 0;
  const inFlight = new Set();
  const results = [];

  async function drainOne() {
    const arr = Array.from(inFlight);
    const wrapped = arr.map((p) =>
      p.then(
        (res) => ({ p, res }),
        (err) => ({ p, err })
      )
    );

    const { p, res, err } = await Promise.race(wrapped);
    inFlight.delete(p);
    if (err) throw err;
    return res;
  }

  while (i < htmlFiles.length) {
    while (inFlight.size < limit && i < htmlFiles.length) {
      const file = htmlFiles[i++];
      const p = (async () => {
        const fullPath = path.join(dir, file);
        const tRead = metrics.now();
        const html = await fsp.readFile(fullPath, "utf8");
        metrics.addTime("readHtmlMs", metrics.now() - tRead);
        if (metrics) {
          metrics.inc("htmlBytes", Buffer.byteLength(html, "utf8"));
        }

        const extra = {
          group_id: unit.group_id,
          case_id: unit.case_id,
          version: unit.version,
        };

        const tCpu = metrics.now();
        const result = await piscina.run({ html, extra });
        metrics.addTime("cpuParseMs", metrics.now() - tCpu);
        return result
      })();

      inFlight.add(p);
    }

    const doc = await drainOne();
    results.push(doc);
  }

  while (inFlight.size) {
    const doc = await drainOne();
    results.push(doc);
  }

  return results;
}
