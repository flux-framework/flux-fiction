from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path
from types import SimpleNamespace

try:
    _tqdm_missing = importlib.util.find_spec("tqdm") is None
except ValueError:
    _tqdm_missing = "tqdm" not in sys.modules

if _tqdm_missing:
    tqdm_stub = types.ModuleType("tqdm")
    tqdm_stub.tqdm = lambda *args, **kwargs: None
    sys.modules["tqdm"] = tqdm_stub

from flux_fiction._adapters.mock.adapter import MockAdapter
from flux_fiction._core.engine import Simulation
from flux_fiction._core.events import EventList
from flux_fiction._core.models import Job
from flux_fiction.api import client
from flux_fiction.api.config import ExperimentConfig
from flux_fiction.api.status import RunStatusWriter


def test_status_snapshot_tracks_submitted_started_and_completed(tmp_path: Path):
    status_path = tmp_path / "status.json"
    adapter = MockAdapter()
    simulation = Simulation(
        adapter=adapter,
        event_list=EventList(),
        job_map={},
        output_dir=str(tmp_path / "output") + "/",
        status=RunStatusWriter(status_path),
        config_file="/tmp/generated.toml",
        source_config_file="/tmp/source.toml",
        trace_file="/tmp/trace.csv",
    )
    adapter.open(simulation)
    adapter.install_resources(SimpleNamespace(nnodes=1, ncpus=1, ngpus=0))

    job = Job(
        nnodes=1,
        ncpus=1,
        submit_time=0.0,
        elapsed_time=1.0,
        timelimit=10.0,
    )
    job.trace_index = 0
    simulation.jobs_total = 1
    simulation.write_status_snapshot(state="running")
    job.insert_apriori_events(simulation)

    simulation.advance()
    adapter.start_reactor()

    payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert payload["state"] == "running"
    assert payload["jobs_total"] == 1
    assert payload["jobs_submitted"] == 1
    assert payload["jobs_started"] == 1
    assert payload["jobs_running"] == 0
    assert payload["jobs_completed"] == 1
    assert payload["time_step"] >= 2
    assert payload["current_sim_time"] == 1.0


def test_engine_writes_summary_file(tmp_path: Path):
    trace_path = tmp_path / "trace.csv"
    trace_path.write_text(
        "JobID,NNodes,NCPUS,Timelimit,Submit,Elapsed\n"
        "1,1,1,00:10:00,2019-01-01T00:00:00,00:00:01\n",
        encoding="utf-8",
    )
    summary_path = tmp_path / "summary.json"
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    cfg = ExperimentConfig(
        job_traces=str(trace_path),
        nnodes=1,
        ncpus=1,
        ngpus=0,
        backend="mock",
        quiet=True,
        output_dir=str(output_dir) + "/",
        summary_file=str(summary_path),
    )

    result = client.run_experiment(cfg, adapter=MockAdapter())

    assert result.ok
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["state"] == "succeeded"
    assert payload["jobs_total"] == 1
    assert payload["jobs_completed"] == 1
    assert payload["makespan_seconds"] >= 0.0
    assert "resource_summary" in payload
