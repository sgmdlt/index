import { pickVersionTs, dirToVersion } from "./utils/dates.js";
import { normPath, dirOfRel } from "./utils/paths.js";

export function toDocument({
  manifestPath,
  relFromRoot,
  meta,
  htmlFiles,
  parsedFiles,
  tag,
}) {
  const relDir = dirOfRel(relFromRoot);
  const preferredId =
    meta && typeof meta.id_final === "string" && meta.id_final.trim()
      ? meta.id_final
      : relFromRoot.replace(/[/\\]manifest\.json$/i, "").replace(/\\/g, "/");

  const group_id = meta?.hashed_id || meta?.ID || preferredId;
  const version = dirToVersion(relDir);
  const version_ts = pickVersionTs(htmlFiles, relDir, meta);

  const files = (parsedFiles || []).map((f) => {
    const obj = {
      file: f.file,
      document_text: f.full_document_texts,
      defendants: f.defendants,
      judge: f.judge,
      participants: f.participants,
    };
    if (f.error) obj.error = f.error;
    return obj;
  });

  const doc_text_full =
    files
      .map((f) => (f.full_document_texts || "").trim())
      .filter(Boolean)
      .join("\n\n") || undefined;

  return {
    files,
    group_id,
    version,
    version_ts,
    is_latest: true,
    id_final: preferredId,
    path_manifest: normPath(manifestPath),
    rel_dir: relDir,
    html_files: htmlFiles,
    meta,
    doc_text_full,
    tag: tag || undefined,
  };
}
