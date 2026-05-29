from __future__ import annotations

import importlib.util
import sys
import types

if importlib.util.find_spec("tqdm") is None:
    tqdm_stub = types.ModuleType("tqdm")
    tqdm_stub.tqdm = lambda *args, **kwargs: None
    sys.modules["tqdm"] = tqdm_stub

from flux_fiction._core.models import Job
from flux_fiction._outputs.vis import _resource_capacities


RABBIT_STORAGE = {
    "resource_type": "ssd",
    "parent_type": "rack",
    "nodes_per_parent": 16,
    "shares_per_parent": 36,
    "share_gib": 453.0,
    "max_parent_gib": 16308.0,
    "parent_count": 72,
}


def test_rabbit_storage_jobspec_uses_gib_for_fluxion_count():
    job = Job(
        nnodes=4,
        ncpus=160,
        submit_time=0,
        elapsed_time=10,
        timelimit=20,
        rabbit_storage_gib=8154.0,
    )
    job.set_rabbit_storage_shape(RABBIT_STORAGE)

    assert job.rabbit_storage_share_count == 18
    assert job.rabbit_storage_request_count == 8154

    rack = job.jobspec["resources"][0]
    children = {child["type"]: child for child in rack["with"]}

    assert rack["type"] == "rack"
    assert rack["count"] == 1
    assert children["node"]["count"] == 4
    assert children["ssd"]["count"] == 8154
    assert children["ssd"]["exclusive"] is True


def test_rabbit_storage_jobspec_splits_large_requests_across_parents():
    job = Job(
        nnodes=4,
        ncpus=160,
        submit_time=0,
        elapsed_time=10,
        timelimit=20,
        rabbit_storage_gib=20000.0,
    )
    job.set_rabbit_storage_shape(RABBIT_STORAGE)

    rack = job.jobspec["resources"][0]
    children = {child["type"]: child for child in rack["with"]}

    assert rack["count"] == 2
    assert children["node"]["count"] == 2
    assert children["ssd"]["count"] == 10000


def test_rabbit_storage_capacity_prefers_storage_size_over_share_count():
    capacities = _resource_capacities({
        "nnodes": 1153,
        "cores_per_node": 96,
        "gpus_per_node": 4,
        "leaf_resources": {
            "ssd": {"count": 2592, "size": 1153692.0, "unit": "GiB"},
        },
        "rabbit_storage": RABBIT_STORAGE,
    })

    assert capacities["ssd"] == 1153692.0
