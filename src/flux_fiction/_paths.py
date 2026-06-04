from __future__ import annotations

import os
from importlib import resources
from pathlib import Path


def _candidate_paths() -> list[Path]:
    candidates: list[Path] = []

    override = os.environ.get("FLUX_FICTION_JOBTAP_SO")
    if override:
        candidates.append(Path(override).expanduser())

    try:
        bundled = resources.files("flux_fiction").joinpath("_native", "emu-jobtap.so")
        candidates.append(Path(str(bundled)))
    except Exception:
        pass

    repo_root = Path(__file__).resolve().parents[2]
    candidates.append(repo_root / "build" / "emu-jobtap.so")

    return candidates


def jobtap_plugin_path() -> Path:
    for candidate in _candidate_paths():
        if candidate.is_file():
            return candidate.resolve()
    raise FileNotFoundError(
        "Could not locate emu-jobtap.so. Set FLUX_FICTION_JOBTAP_SO or build/install Flux Fiction first."
    )
