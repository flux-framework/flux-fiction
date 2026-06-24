from __future__ import annotations

import builtins
import csv
import json
from pathlib import Path
from types import SimpleNamespace

from flux_fiction._outputs import vis


class _DummyAdapter:
    def __init__(self, diagnostics=None, nodelists=None):
        self._diagnostics = diagnostics or {}
        self._nodelists = nodelists or {}

    def get_job_diagnostics(self, jobid):
        return self._diagnostics[jobid]

    def nodelist_lookup(self, jobid):
        return self._nodelists.get(jobid, []), "flux_nodelist"


def _job(jobid, start, finish, jobspec, *, nnodes=1, rabbit_type=None, rabbit_count=0):
    return SimpleNamespace(
        trace_index=jobid,
        nnodes=nnodes,
        jobspec=jobspec,
        rabbit_storage_resource_type=rabbit_type,
        rabbit_storage_request_count=rabbit_count,
        state_transitions={"STARTED": start, "COMPLETED": finish},
    )


def test_match_policy_helpers_and_json_failures(tmp_path: Path):
    config_path = tmp_path / "sched.json"
    config_path.write_text(json.dumps({"sched-fluxion-resource": {"match-policy": "highx"}}), encoding="utf-8")

    assert vis.match_policy_from_config(str(config_path)) == "highx"
    assert vis.match_policy_is_node_exclusive(str(config_path)) is True
    assert vis.match_policy_from_config(str(tmp_path / "missing.json")) == ""


def test_allocation_and_jobspec_helpers_cover_ranges_and_storage():
    r_payload = {
        "execution": {
            "R_lite": [
                {"rank": "0-1", "children": {"core": "0-3", "gpu": "0", "ssd": "0-9"}},
            ]
        }
    }
    counts = vis._allocated_counts_from_r(json.dumps(r_payload))
    assert counts == {"node": 2.0, "core": 8.0, "gpu": 2.0, "ssd": 20.0}

    jobspec = {
        "resources": [
            {
                "type": "node",
                "count": 2,
                "with": [
                    {
                        "type": "slot",
                        "count": 1,
                        "with": [{"type": "core", "count": 4}],
                    },
                    {"type": "gpu", "count": 1},
                ],
            },
            {"type": "ssd", "count": 50},
        ]
    }
    req = vis._jobspec_resource_counts(jobspec)
    assert req["node"] == 2.0
    assert req["core"] == 8.0
    assert req["gpu"] == 2.0
    assert req["ssd"] == 50.0
    assert vis._job_specific_resource_count(
        SimpleNamespace(
            rabbit_storage_resource_type="ssd",
            rabbit_storage_request_count=120,
        ),
        "ssd",
    ) == 120.0


def test_usage_segments_summaries_and_csv_outputs(tmp_path: Path):
    capacities = {"node": 2.0, "core": 8.0}
    rows = [
        {
            "jobid": 1,
            "trace_idx": 10,
            "start": 0.0,
            "finish": 10.0,
            "duration": 10.0,
            "node_exclusive": False,
            "resources": {"node": 1.0, "core": 4.0},
            "r_counts": {"node": 1.0},
            "request_counts": {"core": 4.0},
        },
        {
            "jobid": 2,
            "trace_idx": 11,
            "start": 5.0,
            "finish": 15.0,
            "duration": 10.0,
            "node_exclusive": False,
            "resources": {"node": 1.0, "core": 2.0},
            "r_counts": {"node": 1.0},
            "request_counts": {"core": 2.0},
        },
    ]

    segments = vis.build_usage_segments(rows, capacities)
    summary = vis.resource_summary(rows, capacities)

    assert len(segments) == 3
    assert segments[1]["active_jobs"] == 2
    assert summary["resources"]["node"]["used_hours"] > 0
    assert vis.resource_summary([], capacities)["makespan_hours"] == 0.0

    alloc_csv = tmp_path / "alloc.csv"
    usage_csv = tmp_path / "usage.csv"
    vis.write_allocation_csv(str(alloc_csv), rows, capacities)
    vis.write_usage_csv(str(usage_csv), segments, capacities)

    alloc_rows = list(csv.DictReader(alloc_csv.open()))
    usage_rows = list(csv.DictReader(usage_csv.open()))
    assert alloc_rows[0]["node_used"] == "1"
    assert alloc_rows[0]["core_requested"] == "4"
    assert usage_rows[1]["active_jobs"] == "2"
    assert usage_rows[1]["core_utilization_pct"] == "75.000000"


def test_plot_usage_svg_fallback_and_misc_helpers(tmp_path: Path, monkeypatch):
    segments = [
        {"t0": 0.0, "t1": 60.0, "usage": {"core": 4.0, "ssd": 10.0}},
        {"t0": 60.0, "t1": 180.0, "usage": {"core": 2.0, "ssd": 8.0}},
    ]
    capacities = {"core": 8.0, "ssd": 20.0}
    target = tmp_path / "plot.png"

    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "matplotlib" or name.startswith("matplotlib."):
            raise ImportError("no matplotlib")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    svg_path = vis.plot_usage(segments, capacities, str(target))

    assert svg_path is not None and svg_path.endswith(".svg")
    assert "<svg" in Path(svg_path).read_text(encoding="utf-8")
    assert vis._time_axis(segments)["unit"] == "minutes"
    assert vis._tick_label(0.25) == "0.25"
    assert vis._display_name("ssd") == "Rabbit SSD"
    assert vis._hour_unit("ssd") == "GiB-Hours"
    assert vis._idset_count("1-3,6") == 4
    assert vis._jsonish('{"a":1}') == {"a": 1}
    assert vis._as_float("3.5") == 3.5
    assert vis._fmt(4.0) == "4"


def test_summarize_and_plot_resources_end_to_end(tmp_path: Path, monkeypatch, capsys):
    cfg_path = tmp_path / "sched.json"
    cfg_path.write_text(json.dumps({"sched-fluxion-resource": {"policy": "demoX"}}), encoding="utf-8")
    diagnostics = {
        1: {"kvs": {"R": {"execution": {"R_lite": [{"rank": "0", "children": {"core": "0-3"}}]}}}},
    }
    adapter = _DummyAdapter(diagnostics=diagnostics, nodelists={1: [0]})
    jobspec = {
        "resources": [
            {
                "type": "node",
                "count": 1,
                "with": [{"type": "core", "count": 4}],
            }
        ]
    }
    simulation = SimpleNamespace(job_map={1: _job(1, 0.0, 60.0, jobspec)})
    resource_desc = {"nnodes": 2, "cores_per_node": 4, "gpus_per_node": 0}

    summary = vis.summarize_and_plot_resources(
        simulation,
        adapter,
        resource_desc,
        str(tmp_path / "out"),
        config_json=str(cfg_path),
    )

    assert summary["node_exclusive"] is True
    assert Path(summary["usage_csv"]).exists()
    assert Path(summary["allocation_csv"]).exists()
    captured = capsys.readouterr().out
    assert "Resource usage CSV:" in captured


def test_job_resource_rows_falls_back_when_diagnostics_fail():
    class _BrokenAdapter:
        def get_job_diagnostics(self, _jobid):
            raise RuntimeError("boom")

        def nodelist_lookup(self, _jobid):
            raise RuntimeError("missing")

    simulation = SimpleNamespace(
        job_map={
            1: _job(
                1,
                0.0,
                10.0,
                {"resources": [{"type": "node", "count": 2}]},
                nnodes=2,
            )
        }
    )
    rows = vis._job_resource_rows(
        simulation,
        _BrokenAdapter(),
        {"cores_per_node": 8, "gpus_per_node": 0},
        {"node": 4.0},
        node_exclusive=False,
    )

    assert rows[0]["resources"]["node"] == 4.0
