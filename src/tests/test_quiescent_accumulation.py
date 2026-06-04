from __future__ import annotations

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


class _RecordingAdapter:
    def __init__(self):
        self.simulation = None
        self.accumulate_payloads = []
        self.query_payloads = []
        self.stop_called = False

    def open(self, simulation):
        self.simulation = simulation

    def get_kvs_stats(self):
        return {"dbfile_size": 0, "object_count": 0}

    def accumulate_quiescent(self, payload):
        self.accumulate_payloads.append(payload)

    def query_quiescent(self, payload, return_cb):
        self.query_payloads.append(payload)
        return_cb(None, None)

    def stop_reactor(self):
        self.stop_called = True


def test_subsecond_buckets_accumulate_before_quiescent(tmp_path: Path):
    event_list = EventList()
    event_list.add_event(0.0, lambda: None)
    event_list.add_event(0.2, lambda: None)
    event_list.add_event(2.0, lambda: None)

    adapter = _RecordingAdapter()
    simulation = Simulation(
        adapter=adapter,
        event_list=event_list,
        job_map={},
        output_dir=str(tmp_path) + "/",
    )
    adapter.open(simulation)

    simulation.step_expect[0.0]["submits"] = 1
    simulation.step_expect[0.2]["submits"] = 1
    simulation.step_expect[2.0]["finishes"] = 1

    simulation.advance()

    assert len(adapter.query_payloads) == 2
    assert len(adapter.accumulate_payloads) == 2
    assert "\"submits\": 2" in adapter.accumulate_payloads[0]
    assert "\"finishes\": 0" in adapter.accumulate_payloads[0]
    assert "\"submits\": 0" in adapter.accumulate_payloads[1]
    assert "\"finishes\": 1" in adapter.accumulate_payloads[1]
    assert adapter.stop_called is True
