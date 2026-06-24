from __future__ import annotations

import json
import subprocess
from argparse import Namespace
from pathlib import Path

import pytest

from flux_fiction.cli.support import diagnostics


def _completed(args, *, rc=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(args=args, returncode=rc, stdout=stdout, stderr=stderr)


def test_run_capture_converts_timeout_to_completed_process(monkeypatch):
    def fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=["flux"], timeout=0.5, output="partial", stderr="oops")

    monkeypatch.setattr(diagnostics.subprocess, "run", fake_run)
    monkeypatch.setattr(diagnostics.time, "monotonic", lambda: 10.0)

    proc = diagnostics._run_capture(["flux"], deadline=20.0, command_timeout=1.0)

    assert proc.returncode == 124
    assert proc.stdout == "partial"
    assert "timed out after" in proc.stderr


def test_run_capture_converts_exceptions_to_completed_process(monkeypatch):
    monkeypatch.setattr(diagnostics.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("bad exec")))
    monkeypatch.setattr(diagnostics.time, "monotonic", lambda: 10.0)

    proc = diagnostics._run_capture(["flux"], deadline=20.0, command_timeout=1.0)

    assert proc.returncode == 125
    assert "[exception] OSError: bad exec" in proc.stderr


def test_emergency_dump_writes_artifacts(monkeypatch, tmp_path: Path):
    jobs_payload = json.dumps(
        {
            "jobs": [
                {"jobid": "123", "state": "RUN"},
                {"jobid": "456", "state": "RUN"},
            ]
        }
    )

    def fake_run_capture(args, *, deadline, command_timeout):
        if args[:4] == ["flux", "jobs", "-a", "--json"]:
            return _completed(args, stdout=jobs_payload)
        if args[:3] == ["bash", "-lc", "flux config get | jq"]:
            return _completed(args, stdout='{"queues": []}\n')
        if args[:3] == ["flux", "job", "eventlog"]:
            return _completed(args, stdout="eventlog-body\n", stderr="eventlog-stderr\n")
        if args[:4] == ["flux", "job", "info", "123"] or args[:4] == ["flux", "job", "info", "456"]:
            suffix = args[4]
            if suffix == "R":
                return _completed(args, stdout="R-body\n")
            if suffix == "jobspec":
                return _completed(args, stdout='{"jobspec": 1}\n')
        raise AssertionError(f"Unexpected command: {args}")

    monotonic_values = iter([100.0] * 20)
    monkeypatch.setattr(diagnostics, "_run_capture", fake_run_capture)
    monkeypatch.setattr(diagnostics.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setenv("FLUX_FICTION_EMERGENCY_JOB_LIMIT", "1")

    args = Namespace(
        jobs_path=str(tmp_path / "jobs.json"),
        eventlog_path=str(tmp_path / "eventlogs.txt"),
        alloc_path=str(tmp_path / "alloc.txt"),
        jobspec_path=str(tmp_path / "jobspecs.txt"),
        flux_config_path=str(tmp_path / "flux-config.json"),
        done_path=str(tmp_path / "done.txt"),
    )

    rc = diagnostics.emergency_dump(args)

    assert rc == 0
    assert (tmp_path / "done.txt").read_text(encoding="utf-8") == "ok\n"
    assert json.loads((tmp_path / "jobs.json").read_text(encoding="utf-8"))["jobs"][0]["jobid"] == "123"
    assert '{"queues": []}' in (tmp_path / "flux-config.json").read_text(encoding="utf-8")
    eventlog_text = (tmp_path / "eventlogs.txt").read_text(encoding="utf-8")
    assert "truncated detailed job dumps" in eventlog_text
    assert "eventlog-body" in eventlog_text
    assert "eventlog-stderr" in eventlog_text
    alloc_text = (tmp_path / "alloc.txt").read_text(encoding="utf-8")
    assert '"jobid": "123"' in alloc_text
    assert "R-body" in alloc_text
    jobspec_text = (tmp_path / "jobspecs.txt").read_text(encoding="utf-8")
    assert '{"jobspec": 1}' in jobspec_text


def test_capture_sample_jobspec_success(monkeypatch, tmp_path: Path, capsys):
    responses = iter(
        [
            _completed(
                ["flux", "jobs", "-a", "--json"],
                stdout=json.dumps({"jobs": [{"jobid": "f123"}]}),
            ),
            _completed(
                ["flux", "job", "info", "f123", "jobspec"],
                stdout='{"version": 1}\n',
            ),
        ]
    )
    monkeypatch.setattr(diagnostics.subprocess, "run", lambda *args, **kwargs: next(responses))

    args = Namespace(
        jobs_path=str(tmp_path / "jobs.json"),
        jobid_path=str(tmp_path / "jobid.txt"),
        jobspec_path=str(tmp_path / "jobspec.json"),
    )

    rc = diagnostics.capture_sample_jobspec(args)

    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out.strip() == "f123"
    assert (tmp_path / "jobid.txt").read_text(encoding="utf-8") == "f123\n"
    assert (tmp_path / "jobspec.json").read_text(encoding="utf-8") == '{"version": 1}\n'


def test_capture_sample_jobspec_raises_when_no_jobs(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        diagnostics.subprocess,
        "run",
        lambda *args, **kwargs: _completed(["flux", "jobs", "-a", "--json"], stdout=json.dumps({"jobs": []})),
    )

    args = Namespace(
        jobs_path=str(tmp_path / "jobs.json"),
        jobid_path=str(tmp_path / "jobid.txt"),
        jobspec_path=str(tmp_path / "jobspec.json"),
    )

    with pytest.raises(SystemExit, match="No jobs found"):
        diagnostics.capture_sample_jobspec(args)
