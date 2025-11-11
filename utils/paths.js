import path from "path";

export const normPath = (p) => (p == null ? "" : String(p).replace(/\\/g, "/"));

export const dirOfRel = (rel) => normPath(path.dirname(rel));
