from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
from typing import Any, Optional

from pydantic import BaseModel, Field, ValidationError

try:
    from pydantic import ConfigDict, model_validator

    _PYDANTIC_V2 = True
except ImportError:
    from pydantic import root_validator

    ConfigDict = None
    _PYDANTIC_V2 = False


class ParallelValidationError(ValueError):
    pass


def _load_toml(path: Path) -> dict[str, Any]:
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore

    with path.open("rb") as f:
        data = tomllib.load(f)
    if not isinstance(data, dict):
        raise ParallelValidationError(f"Parallel manifest is not a TOML table: {path}")
    return data


def _resolve_input_path(value: str, *, manifest_dir: Path) -> str:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (manifest_dir / path).resolve()
    else:
        path = path.resolve()
    if not path.exists():
        raise ParallelValidationError(f"Referenced path does not exist: {path}")
    return str(path)


def _resolve_output_path(value: str, *, manifest_dir: Path) -> str:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (manifest_dir / path).resolve()
    else:
        path = path.resolve()
    return str(path)


def _slugify(name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", name.strip())
    slug = slug.strip("-._")
    return slug or "run"


@dataclass(frozen=True)
class ParallelSettings:
    max_concurrent: int = 1
    fail_fast: bool = False
    progress_mode: str = "summary"
    default_no_faketime: bool = False
    default_broker_log_level: int = 6
    output_root: str = "./parallel-runs"
    summary_interval: float = 5.0


@dataclass(frozen=True)
class ParallelRun:
    name: str
    config_file: str
    job_traces: Optional[str] = None
    config_json: Optional[str] = None
    resource_file: Optional[str] = None
    resource_R: Optional[str] = None
    tag: Optional[str] = None
    no_faketime: Optional[bool] = None
    broker_log_level: Optional[int] = None
    metadata: Optional[dict[str, Any]] = None


@dataclass(frozen=True)
class ParallelManifest:
    version: int
    parallel: ParallelSettings
    run: list[ParallelRun]
    manifest_path: str


@dataclass(frozen=True)
class ParallelRunPlan:
    ordinal: int
    name: str
    slug: str
    config_file: str
    job_traces: Optional[str]
    config_json: Optional[str]
    resource_file: Optional[str]
    resource_R: Optional[str]
    tag: Optional[str]
    no_faketime: bool
    broker_log_level: int
    metadata: dict[str, Any]
    run_root: str
    child_run_dir: str
    stampfile: str


@dataclass(frozen=True)
class ResolvedParallelPlan:
    manifest_path: str
    output_root: str
    max_concurrent: int
    fail_fast: bool
    progress_mode: str
    summary_interval: float
    shard_index: Optional[int]
    shard_count: Optional[int]
    runs: list[ParallelRunPlan]

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "manifest_path": self.manifest_path,
            "output_root": self.output_root,
            "max_concurrent": self.max_concurrent,
            "fail_fast": self.fail_fast,
            "progress_mode": self.progress_mode,
            "summary_interval": self.summary_interval,
            "shard_index": self.shard_index,
            "shard_count": self.shard_count,
            "runs": [
                {
                    "ordinal": run.ordinal,
                    "name": run.name,
                    "slug": run.slug,
                    "config_file": run.config_file,
                    "job_traces": run.job_traces,
                    "config_json": run.config_json,
                    "resource_file": run.resource_file,
                    "resource_R": run.resource_R,
                    "tag": run.tag,
                    "no_faketime": run.no_faketime,
                    "broker_log_level": run.broker_log_level,
                    "metadata": run.metadata,
                    "run_root": run.run_root,
                    "child_run_dir": run.child_run_dir,
                    "stampfile": run.stampfile,
                }
                for run in self.runs
            ],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), indent=2, sort_keys=True)


class ParallelSettingsModel(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="forbid")
    else:
        class Config:
            extra = "forbid"

    max_concurrent: int = Field(default=1, ge=1)
    fail_fast: bool = False
    progress_mode: str = "summary"
    default_no_faketime: bool = False
    default_broker_log_level: int = Field(default=6, ge=0)
    output_root: str = "./parallel-runs"
    summary_interval: float = Field(default=5.0, gt=0)

    if _PYDANTIC_V2:
        @model_validator(mode="after")
        def _checks(self):
            if self.progress_mode not in {"summary", "quiet"}:
                raise ValueError("progress_mode must be one of: summary, quiet")
            return self
    else:
        @root_validator(skip_on_failure=True)
        def _checks(cls, values):
            if values.get("progress_mode") not in {"summary", "quiet"}:
                raise ValueError("progress_mode must be one of: summary, quiet")
            return values


class ParallelRunModel(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="forbid")
    else:
        class Config:
            extra = "forbid"

    name: str
    config_file: str
    job_traces: Optional[str] = None
    config_json: Optional[str] = None
    resource_file: Optional[str] = None
    resource_R: Optional[str] = None
    tag: Optional[str] = None
    no_faketime: Optional[bool] = None
    broker_log_level: Optional[int] = Field(default=None, ge=0)
    metadata: dict[str, Any] = Field(default_factory=dict)

    if _PYDANTIC_V2:
        @model_validator(mode="after")
        def _checks(self):
            if not self.name.strip():
                raise ValueError("run name must not be empty")
            if self.resource_file and self.resource_R:
                raise ValueError("Use only one of resource_file or resource_R.")
            return self
    else:
        @root_validator(skip_on_failure=True)
        def _checks(cls, values):
            if not (values.get("name") or "").strip():
                raise ValueError("run name must not be empty")
            if values.get("resource_file") and values.get("resource_R"):
                raise ValueError("Use only one of resource_file or resource_R.")
            return values


class ParallelManifestModel(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="forbid")
    else:
        class Config:
            extra = "forbid"

    version: int
    parallel: ParallelSettingsModel
    run: list[ParallelRunModel]

    if _PYDANTIC_V2:
        @model_validator(mode="after")
        def _checks(self):
            if self.version != 1:
                raise ValueError(f"Unsupported manifest version {self.version!r}; expected 1")
            if not self.run:
                raise ValueError("At least one [[run]] entry is required.")
            names = [entry.name for entry in self.run]
            if len(set(names)) != len(names):
                raise ValueError("Run names must be unique.")
            return self
    else:
        @root_validator(skip_on_failure=True)
        def _checks(cls, values):
            if values.get("version") != 1:
                raise ValueError(
                    f"Unsupported manifest version {values.get('version')!r}; expected 1"
                )
            runs = values.get("run") or []
            if not runs:
                raise ValueError("At least one [[run]] entry is required.")
            names = [entry.name for entry in runs]
            if len(set(names)) != len(names):
                raise ValueError("Run names must be unique.")
            return values


def _model_to_data(model: BaseModel) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def _validate_manifest(data: dict[str, Any]) -> ParallelManifestModel:
    try:
        if _PYDANTIC_V2:
            return ParallelManifestModel.model_validate(data)
        return ParallelManifestModel.parse_obj(data)
    except ValidationError as exc:
        raise ParallelValidationError(str(exc)) from exc


def load_parallel_manifest(path: str | os.PathLike[str]) -> ParallelManifest:
    manifest_path = Path(path).expanduser().resolve()
    if not manifest_path.exists():
        raise FileNotFoundError(f"Parallel manifest not found: {manifest_path}")

    raw = _load_toml(manifest_path)
    manifest_dir = manifest_path.parent

    parallel_data = dict(raw.get("parallel") or {})
    if "output_root" in parallel_data and parallel_data["output_root"] is not None:
        parallel_data["output_root"] = _resolve_output_path(
            str(parallel_data["output_root"]),
            manifest_dir=manifest_dir,
        )
    else:
        parallel_data["output_root"] = str((manifest_dir / "parallel-runs").resolve())

    runs_data: list[dict[str, Any]] = []
    for entry in raw.get("run") or []:
        run_data = dict(entry)
        for key in ("config_file", "job_traces", "config_json", "resource_file", "resource_R"):
            if run_data.get(key):
                run_data[key] = _resolve_input_path(str(run_data[key]), manifest_dir=manifest_dir)
        runs_data.append(run_data)

    validated = _validate_manifest(
        {
            "version": raw.get("version"),
            "parallel": parallel_data,
            "run": runs_data,
        }
    )
    validated_data = _model_to_data(validated)
    return ParallelManifest(
        version=int(validated_data["version"]),
        parallel=ParallelSettings(**validated_data["parallel"]),
        run=[ParallelRun(**entry) for entry in validated_data["run"]],
        manifest_path=str(manifest_path),
    )


def resolve_parallel_plan(
    manifest: ParallelManifest,
    *,
    max_concurrent: int | None = None,
    fail_fast: bool | None = None,
    output_root: str | os.PathLike[str] | None = None,
    summary_interval: float | None = None,
    shard_index: int | None = None,
    shard_count: int | None = None,
) -> ResolvedParallelPlan:
    if shard_index is not None and shard_count is None:
        raise ParallelValidationError("shard_count is required when shard_index is set")
    if shard_count is not None and shard_index is None:
        raise ParallelValidationError("shard_index is required when shard_count is set")
    if shard_count is not None and shard_count < 1:
        raise ParallelValidationError("shard_count must be >= 1")
    if shard_index is not None and (shard_index < 0 or shard_index >= int(shard_count)):
        raise ParallelValidationError("shard_index must satisfy 0 <= shard_index < shard_count")

    effective_output_root = (
        Path(output_root).expanduser().resolve()
        if output_root is not None
        else Path(manifest.parallel.output_root).expanduser().resolve()
    )
    effective_max_concurrent = int(max_concurrent or manifest.parallel.max_concurrent)
    effective_fail_fast = manifest.parallel.fail_fast if fail_fast is None else bool(fail_fast)
    effective_summary_interval = float(
        manifest.parallel.summary_interval if summary_interval is None else summary_interval
    )

    selected_runs = manifest.run
    if shard_count is not None:
        selected_runs = [
            run for idx, run in enumerate(manifest.run) if idx % int(shard_count) == int(shard_index)
        ]

    plans: list[ParallelRunPlan] = []
    for ordinal, run in enumerate(selected_runs, start=1):
        slug = _slugify(run.name)
        run_root = effective_output_root / "runs" / f"{ordinal:04d}_{slug}"
        child_run_dir = run_root / "child"
        stampfile = child_run_dir / "faketime_stamp"
        plans.append(
            ParallelRunPlan(
                ordinal=ordinal,
                name=run.name,
                slug=slug,
                config_file=run.config_file,
                job_traces=run.job_traces,
                config_json=run.config_json,
                resource_file=run.resource_file,
                resource_R=run.resource_R,
                tag=run.tag,
                no_faketime=(
                    manifest.parallel.default_no_faketime
                    if run.no_faketime is None
                    else bool(run.no_faketime)
                ),
                broker_log_level=(
                    manifest.parallel.default_broker_log_level
                    if run.broker_log_level is None
                    else int(run.broker_log_level)
                ),
                metadata=dict(run.metadata or {}),
                run_root=str(run_root),
                child_run_dir=str(child_run_dir),
                stampfile=str(stampfile),
            )
        )

    return ResolvedParallelPlan(
        manifest_path=manifest.manifest_path,
        output_root=str(effective_output_root),
        max_concurrent=effective_max_concurrent,
        fail_fast=effective_fail_fast,
        progress_mode=manifest.parallel.progress_mode,
        summary_interval=effective_summary_interval,
        shard_index=shard_index,
        shard_count=shard_count,
        runs=plans,
    )
