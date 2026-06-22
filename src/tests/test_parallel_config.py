from __future__ import annotations

from pathlib import Path

import pytest

from flux_fiction.cli import run_ff_parallel
from flux_fiction.parallel import ParallelValidationError, load_parallel_manifest, resolve_parallel_plan


def _write_manifest(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


def test_load_parallel_manifest_resolves_relative_paths(tmp_path):
    config = tmp_path / "config.toml"
    trace = tmp_path / "trace.csv"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    trace.write_text("Submit,Elapsed,Timelimit,NNodes,NCPUS\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 2
output_root = "./parallel-out"

[[run]]
name = "alpha"
config_file = "./config.toml"
job_traces = "./trace.csv"
""".strip()
        + "\n",
    )

    manifest = load_parallel_manifest(manifest_path)

    assert manifest.parallel.output_root == str((tmp_path / "parallel-out").resolve())
    assert manifest.run[0].config_file == str(config.resolve())
    assert manifest.run[0].job_traces == str(trace.resolve())


def test_load_parallel_manifest_rejects_duplicate_names(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 1

[[run]]
name = "dup"
config_file = "./config.toml"

[[run]]
name = "dup"
config_file = "./config.toml"
""".strip()
        + "\n",
    )

    with pytest.raises(ParallelValidationError, match="Run names must be unique"):
        load_parallel_manifest(manifest_path)


def test_load_parallel_manifest_rejects_missing_paths(tmp_path):
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 1

[[run]]
name = "missing"
config_file = "./does-not-exist.toml"
""".strip()
        + "\n",
    )

    with pytest.raises(ParallelValidationError, match="Referenced path does not exist"):
        load_parallel_manifest(manifest_path)


def test_resolve_parallel_plan_applies_defaults_and_paths(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 3
default_no_faketime = true
default_broker_log_level = 9
output_root = "./planned-output"

[[run]]
name = "alpha run"
config_file = "./config.toml"
""".strip()
        + "\n",
    )
    manifest = load_parallel_manifest(manifest_path)

    plan = resolve_parallel_plan(manifest)

    assert plan.max_concurrent == 3
    assert len(plan.runs) == 1
    run = plan.runs[0]
    assert run.no_faketime is True
    assert run.broker_log_level == 9
    assert run.run_root.endswith("/runs/0001_alpha-run")
    assert run.child_run_dir.endswith("/runs/0001_alpha-run/child")
    assert run.stampfile.endswith("/runs/0001_alpha-run/child/faketime_stamp")


def test_resolve_parallel_plan_supports_sharding(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 2

[[run]]
name = "run-a"
config_file = "./config.toml"

[[run]]
name = "run-b"
config_file = "./config.toml"

[[run]]
name = "run-c"
config_file = "./config.toml"
""".strip()
        + "\n",
    )
    manifest = load_parallel_manifest(manifest_path)

    plan = resolve_parallel_plan(manifest, shard_index=1, shard_count=2)

    assert [run.name for run in plan.runs] == ["run-b"]


def test_run_ff_parallel_dry_run_prints_plan(tmp_path, capsys):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 1

[[run]]
name = "alpha"
config_file = "./config.toml"
""".strip()
        + "\n",
    )

    rc = run_ff_parallel.main([str(manifest_path), "--dry-run"])

    captured = capsys.readouterr()
    assert rc == 0
    assert "Planned runs:      1" in captured.out
    assert "Dry run requested; not launching child runs." in captured.out


def test_run_ff_parallel_requires_dry_run_in_phase3(tmp_path, capsys):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 1

[[run]]
name = "alpha"
config_file = "./config.toml"
""".strip()
        + "\n",
    )

    rc = run_ff_parallel.main([str(manifest_path)])

    captured = capsys.readouterr()
    assert rc == 2
    assert "Parallel execution is not implemented yet in Phase 3" in captured.err
