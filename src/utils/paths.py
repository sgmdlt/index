"""Path normalization utilities shared across the pipeline."""

import os


def norm_path(p: str) -> str:
    return str(p).replace("\\", "/")


def dir_of_rel(rel: str) -> str:
    return norm_path(os.path.dirname(rel))
