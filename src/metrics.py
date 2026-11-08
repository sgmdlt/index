"""Performance metrics collector. Ports metrics.js."""

import resource
import time


class Metrics:
    def __init__(self, log_every_sec: float = 5.0):
        self._start = time.monotonic()
        self._last_log = self._start
        self._log_every = log_every_sec

        self.counters = {
            "unitsSeen": 0,
            "htmlFilesSeen": 0,
            "docsPrepared": 0,
            "docsIndexed": 0,
            "bulkCalls": 0,
            "bulkFailedDocs": 0,
            "bulkDocsTotal": 0,
            "parseErrors": 0,
            "indexErrors": 0,
            "htmlBytes": 0,
        }
        self.timers = {
            "parseMs": 0.0,
            "toDocMs": 0.0,
            "bulkMs": 0.0,
            "readHtmlMs": 0.0,
            "manifestFetchMs": 0.0,
            "cpuParseMs": 0.0,
        }
        self._last_html_bytes = 0
        self._last_read_html_ms = 0.0

    def inc(self, key: str, n: int = 1) -> None:
        self.counters[key] = self.counters.get(key, 0) + n

    def add_time(self, key: str, ms: float) -> None:
        self.timers[key] = self.timers.get(key, 0.0) + ms

    def now_ms(self) -> float:
        return time.monotonic() * 1000.0

    def log(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self._last_log) < self._log_every:
            return
        self._last_log = now

        elapsed = max(0.001, now - self._start)
        c = self.counters
        t = self.timers

        docs_per_sec = c["docsIndexed"] / elapsed
        units_per_sec = c["unitsSeen"] / elapsed
        avg_bulk = c["bulkDocsTotal"] / c["bulkCalls"] if c["bulkCalls"] else 0.0
        total_mb = c["htmlBytes"] / (1024 * 1024)
        mb_ps = total_mb / elapsed
        active_read_mb_ps = total_mb / max(0.001, t["readHtmlMs"] / 1000)

        delta_bytes = c["htmlBytes"] - self._last_html_bytes
        delta_read_ms = t["readHtmlMs"] - self._last_read_html_ms
        window_mb_ps = (delta_bytes / (1024 * 1024)) / max(0.001, delta_read_ms / 1000)
        self._last_html_bytes = c["htmlBytes"]
        self._last_read_html_ms = t["readHtmlMs"]

        try:
            ru = resource.getrusage(resource.RUSAGE_SELF)
            rss_mb = ru.ru_maxrss / 1024  # Linux: kB → MB
        except Exception:
            rss_mb = 0.0

        parts = [
            f"rss_MB={rss_mb:.1f}",
            f"time={elapsed:.1f}s",
            f"units={c['unitsSeen']}",
            f"html={c['htmlFilesSeen']}",
            f"docs_prepared={c['docsPrepared']}",
            f"docs_indexed={c['docsIndexed']}",
            f"avg_bulk_size={avg_bulk:.1f}",
            f"bulk_calls={c['bulkCalls']}",
            f"bulk_failed_docs={c['bulkFailedDocs']}",
            f"rate_docs/s={docs_per_sec:.1f}",
            f"rate_units/s={units_per_sec:.2f}",
            f"read_ms={t['readHtmlMs']:.0f}",
            f"active_read_MBps={active_read_mb_ps:.2f}",
            f"window_read_MBps={window_mb_ps:.2f}",
            f"cpu_parse_ms={t['cpuParseMs']:.0f}",
            f"parse_ms={t['parseMs']:.0f}",
            f"toDoc_ms={t['toDocMs']:.0f}",
            f"bulk_ms={t['bulkMs']:.0f}",
            f"manifest_ms={t['manifestFetchMs']:.0f}",
            f"html_MB={total_mb:.1f}",
            f"html_MBps={mb_ps:.2f}",
        ]
        print(" | ".join(parts))
