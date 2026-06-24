from __future__ import annotations

import csv
import importlib.util
import sys
import types
from pathlib import Path

try:
    _tqdm_missing = importlib.util.find_spec("tqdm") is None
except ValueError:
    _tqdm_missing = "tqdm" not in sys.modules

if _tqdm_missing:
    tqdm_stub = types.ModuleType("tqdm")
    tqdm_stub.tqdm = lambda *args, **kwargs: None
    sys.modules["tqdm"] = tqdm_stub

from flux_fiction._core.engine import Simulation
from flux_fiction._core.events import EventList
from flux_fiction._core.models import Job


class _SnapshotAdapter:
    def __init__(self):
        self.requests = []

    def get_scheduler_state(self, jobid):
        self.requests.append(jobid)
        return {
            "state": "running",
            "flux_t_submit": 10.0,
            "flux_t_run": 11.0,
            "flux_t_cleanup": "",
            "flux_expiration": 70.0,
            "flux_duration": 60.0,
            "flux_nodelist": "0",
            "annotations": '{"sched":{"queue":"default"}}',
        }


def test_dump_scheduler_state_uses_adapter_snapshot(tmp_path: Path):
    job = Job(
        nnodes=1,
        ncpus=1,
        submit_time=0.0,
        elapsed_time=5.0,
        timelimit=10.0,
    )
    job.trace_index = 7
    job._jobid = 42
    job.record_state_transition("SUBMITTED", 0.0)
    job.record_state_transition("STARTED", 1.0)
    job.record_state_transition("COMPLETED", 6.0)
    job.start_time = 1.0

    adapter = _SnapshotAdapter()
    simulation = Simulation(
        adapter=adapter,
        event_list=EventList(),
        job_map={42: job},
        output_dir=str(tmp_path) + "/",
    )
    simulation.current_time = 3.0

    simulation.dump_scheduler_state(label="checkpoint")

    outfile = tmp_path / "scheduler_state_log.csv"
    assert outfile.exists()
    assert adapter.requests == [42]

    with outfile.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 1
    row = rows[0]
    assert row["label"] == "checkpoint"
    assert row["jobid"] == "42"
    assert row["trace_idx"] == "7"
    assert row["state"] == "running"
    assert row["flux_t_submit"] == "10.0"
    assert row["flux_t_run"] == "11.0"
    assert row["flux_nodelist"] == "0"
    assert row["annotations"] == '{"sched":{"queue":"default"}}'
