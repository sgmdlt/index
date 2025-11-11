export function htmlNameToIso(name) {
  if (typeof name !== "string") return null;
  const m = name.match(/^(\d{2})-(\d{2})-(\d{4})\.(\d{2}):(\d{2}):(\d{2})/);
  if (!m) return null;
  const [, dd, MM, yyyy, hh, mm, ss] = m;
  const iso = `${yyyy}-${MM}-${dd}T${hh}:${mm}:${ss}Z`;
  const t = Date.parse(iso);
  return Number.isFinite(t) ? new Date(t).toISOString() : null;
}

export function dirToIso(relDir) {
  const m = String(relDir).match(/(^|\/)(\d{4})\/(\d{2})\/(\d{2})(\/|$)/);
  if (!m) return null;
  const [, , yyyy, MM, dd] = m;
  const iso = `${yyyy}-${MM}-${dd}T00:00:00Z`;
  const t = Date.parse(iso);
  return Number.isFinite(t) ? new Date(t).toISOString() : null;
}

export function manifestToIso(meta) {
  const cand = meta?.entry_date_to || meta?.date_reg;
  const m = cand && String(cand).match(/^(\d{2})\.(\d{2})\.(\d{4})/);
  if (!m) return null;
  const [, dd, MM, yyyy] = m;
  const iso = `${yyyy}-${MM}-${dd}T00:00:00Z`;
  const t = Date.parse(iso);
  return Number.isFinite(t) ? new Date(t).toISOString() : null;
}

export function pickVersionTs(htmlFiles, relDir, meta) {
  const fromHtml = (htmlFiles || []).map(htmlNameToIso).filter(Boolean).sort();
  return (
    fromHtml.at(-1) ||
    dirToIso(relDir) ||
    manifestToIso(meta) ||
    new Date().toISOString()
  );
}

export function dirToVersion(relDir) {
  const base = String(relDir).split("/").pop() || "";
  const m = base.match(/-(\d+)$|_(\d+)$|(\d+)$/);
  const num = m ? m[1] || m[2] || m[3] : null;
  return num ? parseInt(num, 10) : 1;
}
