from __future__ import annotations

import json
from types import SimpleNamespace

from flux_fiction._adapters.mock.adapter import (
    MockAdapter,
    MockDiagnosticsConfig,
    MockQuiescenceConfig,
    MockQuiescenceStep,
    MockResourceConfig,
    MockScenario,
)


class _RecordingSimulation:
    def __init__(self, job_map=None):
        self.job_map = job_map or {}
        self.started: list[int] = []

    def start_job(self, jobid: int):
        self.started.append(jobid)


def _jobspec(nnodes: int = 1) -> dict:
    return {"resources": [{"type": "node", "count": nnodes}]}


def test_mock_adapter_quiescence_script_controls_flush_and_start_count():
    scenario = MockScenario(
        quiescence=MockQuiescenceConfig(
            script=(
                MockQuiescenceStep(flush_starts=False),
                MockQuiescenceStep(max_starts=1),
            )
        )
    )
    adapter = MockAdapter(scenario=scenario)
    simulation = _RecordingSimulation()
    adapter.open(simulation)
    adapter.install_resources(SimpleNamespace(nnodes=2, ncpus=4, ngpus=0))

    job1 = adapter.submit_job(_jobspec())
    job2 = adapter.submit_job(_jobspec())

    callbacks: list[str] = []
    adapter.query_quiescent({}, lambda _fut, _arg: callbacks.append("first"))
    adapter.start_reactor()

    assert callbacks == ["first"]
    assert simulation.started == []
    assert [job.jobid for job in adapter._queue] == [job1, job2]

    adapter.query_quiescent({}, lambda _fut, _arg: callbacks.append("second"))
    adapter.start_reactor()

    assert callbacks == ["first", "second"]
    assert simulation.started == [job1]
    assert [job.jobid for job in adapter._queue] == [job2]
    assert len(adapter.applied_quiescence_steps) == 2
    assert adapter.applied_quiescence_steps[0].flush_starts is False
    assert adapter.applied_quiescence_steps[1].max_starts == 1


def test_mock_adapter_rich_diagnostics_synthesize_kvs_and_completion_status():
    scenario = MockScenario(
        diagnostics=MockDiagnosticsConfig(
            include_kvs=True,
            include_queue_status=True,
            use_simulated_timestamps=True,
        ),
        completion_status_by_submit_index={1: 17},
        resources=MockResourceConfig(
            leaf_resources={"ssd": {"count": 4, "size": 512}},
            rabbit_storage={"resource_type": "ssd", "parent_count": 2, "max_parent_gib": 256},
        ),
    )
    adapter = MockAdapter(scenario=scenario)

    jobid = adapter.submit_job(_jobspec())
    job = SimpleNamespace(
        nnodes=1,
        elapsed_time=60.0,
        jobspec=_jobspec(),
        state_transitions={
            "SUBMITTED": 1.0,
            "STARTED": 2.0,
            "COMPLETED": 10.0,
        },
    )
    simulation = _RecordingSimulation(job_map={jobid: job})
    adapter.open(simulation)
    adapter.install_resources(SimpleNamespace(nnodes=2, ncpus=4, ngpus=1))

    adapter.query_quiescent({}, lambda _fut, _arg: None)
    adapter.start_reactor()
    adapter.ack_complete(jobid)

    diag = adapter.get_job_diagnostics(jobid)
    assert diag["state"] == "failed"
    assert diag["queue_status"]["queued"] == 0
    assert diag["queue_status"]["running"] == 0
    assert diag["kvs"]["jobspec"] == _jobspec()
    assert diag["kvs"]["R"]["execution"]["R_lite"]

    events = [json.loads(line) for line in diag["eventlog"]["eventlog"].splitlines() if line.strip()]
    finish = next(event for event in events if event["name"] == "finish")
    assert finish["context"]["status"] == 17

    scheduler_state = adapter.get_scheduler_state(jobid)
    assert scheduler_state["state"] == "failed"

    resource_desc = adapter.describe_resources(SimpleNamespace(nnodes=2, ncpus=4, ngpus=1))
    assert resource_desc["leaf_resources"]["ssd"]["count"] == 4
    assert resource_desc["rabbit_storage"]["parent_count"] == 2
