from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from flux_fiction.cli.support import run_api_experiment


class _RecordingStatus:
    def __init__(self, path):
        self.path = path
        self.updates: list[dict] = []

    def update(self, **kwargs):
        self.updates.append(kwargs)


def test_run_api_experiment_build_parser_accepts_aliases():
    parser = run_api_experiment.build_parser()
    args = parser.parse_args(
        [
            "--config-file",
            "config.toml",
            "--faketime-timestamp-file",
            "stamp.txt",
            "--faketime-no-seed",
            "--faketime-near-event-threshold",
            "0.5",
        ]
    )

    assert args.config_file == "config.toml"
    assert args.faketime_timestamp_file == "stamp.txt"
    assert args.faketime_seed is False
    assert args.faketime_near_event_threshold == 0.5


def test_run_api_experiment_main_updates_status_and_returns_zero(monkeypatch, tmp_path: Path):
    cfg = SimpleNamespace(
        status_file=str(tmp_path / "status.json"),
        config_file=str(tmp_path / "generated.toml"),
        source_config_file=str(tmp_path / "source.toml"),
        job_traces=str(tmp_path / "trace.csv"),
    )
    status_instances: list[_RecordingStatus] = []

    def fake_status_writer(path):
        status = _RecordingStatus(path)
        status_instances.append(status)
        return status

    monkeypatch.setattr(run_api_experiment, "from_toml", lambda args: cfg)
    monkeypatch.setattr(run_api_experiment, "RunStatusWriter", fake_status_writer)
    monkeypatch.setattr(
        run_api_experiment.client,
        "run_experiment",
        lambda passed_cfg, status=None: SimpleNamespace(ok=True, message="done", cfg=passed_cfg, status=status),
    )

    rc = run_api_experiment.main(["--config-file", str(tmp_path / "config.toml")])

    assert rc == 0
    assert len(status_instances) == 1
    assert status_instances[0].updates == [
        {
            "state": "running",
            "config_file": cfg.config_file,
            "source_config_file": cfg.source_config_file,
            "trace_file": cfg.job_traces,
        }
    ]


def test_run_api_experiment_main_returns_one_on_failure(monkeypatch, tmp_path: Path):
    cfg = SimpleNamespace(
        status_file=str(tmp_path / "status.json"),
        config_file=str(tmp_path / "generated.toml"),
        source_config_file=str(tmp_path / "source.toml"),
        job_traces=str(tmp_path / "trace.csv"),
    )

    monkeypatch.setattr(run_api_experiment, "from_toml", lambda args: cfg)
    monkeypatch.setattr(run_api_experiment, "RunStatusWriter", _RecordingStatus)
    monkeypatch.setattr(
        run_api_experiment.client,
        "run_experiment",
        lambda passed_cfg, status=None: SimpleNamespace(ok=False, message="boom", cfg=passed_cfg, status=status),
    )

    rc = run_api_experiment.main(["--config-file", str(tmp_path / "config.toml")])

    assert rc == 1
