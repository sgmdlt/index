import { performance } from "perf_hooks";

export function createMetrics({ logEveryMs = 5000 } = {}) {
  const startTs = performance.now();
  let lastLogTs = startTs;

    const counters = {
  unitsSeen: 0,
  htmlFilesSeen: 0,
  docsPrepared: 0,
  docsIndexed: 0,
  bulkCalls: 0,
  bulkFailedDocs: 0,
  bulkDocsTotal: 0,
  parseErrors: 0,
  indexErrors: 0,
};

  const timers = {
    parseMs: 0,
    toDocMs: 0,
    bulkMs: 0,
  };

  function inc(key, n = 1) {
    if (typeof counters[key] !== "number") counters[key] = 0;
    counters[key] += n;
  }

  function addTime(key, ms) {
    if (typeof timers[key] !== "number") timers[key] = 0;
    timers[key] += ms;
  }

  function now() {
    return performance.now();
  }

  function snapshot() {
    const t = now();
    const elapsedSec = Math.max(0.001, (t - startTs) / 1000);

    const docsPerSec = counters.docsIndexed / elapsedSec;
    const unitsPerSec = counters.unitsSeen / elapsedSec;

    return {
      elapsedSec,
      docsPerSec,
      unitsPerSec,
      counters: { ...counters },
      timers: { ...timers },
    };
  }

  function log(force = false) {
    const t = now();
    if (!force && t - lastLogTs < logEveryMs) return;
    lastLogTs = t;

    const s = snapshot();
    const c = s.counters;
    const avgBulk = counters.bulkCalls ? counters.bulkDocsTotal / counters.bulkCalls : 0;

// добавь в строку лога:
    console.log(
    [
        `time=${s.elapsedSec.toFixed(1)}s`,
        `units=${c.unitsSeen}`,
        `html=${c.htmlFilesSeen}`,
        `docs_prepared=${c.docsPrepared}`,
        `docs_indexed=${c.docsIndexed}`,
        `avg_bulk_size=${avgBulk.toFixed(1)}`,
        `bulk_calls=${c.bulkCalls}`,
        `bulk_failed_docs=${c.bulkFailedDocs}`,
        `rate_docs/s=${s.docsPerSec.toFixed(1)}`,
        `rate_units/s=${s.unitsPerSec.toFixed(2)}`,
        `parse_ms=${s.timers.parseMs.toFixed(0)}`,
        `toDoc_ms=${s.timers.toDocMs.toFixed(0)}`,
        `bulk_ms=${s.timers.bulkMs.toFixed(0)}`,
      ].join(" | ")
    );
  }

  return { inc, addTime, now, log, snapshot };
}
