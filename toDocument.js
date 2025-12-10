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
  const preferredId = relFromRoot.replace(/[/\\]manifest\.json$/i, "").replace(/\//g, "_");

  const group_id = meta?.hashed_id || meta?.ID || preferredId;
  const version = dirToVersion(relDir);
  const version_ts = pickVersionTs(htmlFiles, relDir, meta);
  const files = parsedFiles;

  return {
    files,
    group_id,
    version,
    version_ts,
    id_final: preferredId,
    path_manifest: normPath(manifestPath),
    rel_dir: relDir,
    html_files: htmlFiles,
    meta,
    tag: tag || undefined,
  };
}
