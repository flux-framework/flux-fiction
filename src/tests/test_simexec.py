from __future__ import annotations

from types import SimpleNamespace

from flux_fiction._exec.simexec import SimpleExec


def test_simpleexec_tracks_makespan_and_resource_usage():
    execu = SimpleExec(num_nodes=4, cores_per_node=8, gpus_per_node=2, exclusive=False)
    sim = SimpleNamespace(current_time=10.0)
    job = SimpleNamespace(nnodes=2, ncpus=8, ngpus=2, elapsed_time=60.0)

    execu.submit_job(sim, job)
    execu.start_job(sim, job)
    assert execu.num_free_nodes == 2

    sim.current_time = 70.0
    execu.complete_job(sim, job)

    assert execu.num_free_nodes == 4
    assert execu.used_core_hours == (8 * 60.0) / 3600
    assert execu.used_gpu_hours == (2 * 60.0) / 3600
    assert execu.makespan.beginning == 10.0
    assert execu.makespan.end == 70.0


def test_simpleexec_logs_oversubscription_and_prints_summary(caplog, capsys):
    execu = SimpleExec(num_nodes=1, cores_per_node=4, gpus_per_node=1, exclusive=False)
    sim = SimpleNamespace(current_time=0.0)
    oversized = SimpleNamespace(nnodes=2, ncpus=12, ngpus=4, elapsed_time=30.0)

    execu.submit_job(sim, oversized)
    execu.start_job(sim, oversized)
    assert "Scheduler over-subscribed nodes" in caplog.text
    assert "Scheduler over-subscribed cores on the node" in caplog.text
    assert "Scheduler over-subscribed GPUs on the node" in caplog.text

    sim.current_time = 30.0
    execu.complete_job(sim, oversized)
    execu.post_analysis(sim)

    output = capsys.readouterr().out
    assert "Makespan (hours):" in output
    assert "Average Core-Utilization:" in output
    assert "Average GPU-Utilization:" in output


def test_simpleexec_exclusive_accounting_and_zero_makespan_errors(caplog, capsys):
    execu = SimpleExec(num_nodes=2, cores_per_node=6, gpus_per_node=3, exclusive=True)
    sim = SimpleNamespace(current_time=50.0)
    job = SimpleNamespace(nnodes=1, ncpus=2, ngpus=1, elapsed_time=120.0)

    execu.submit_job(sim, job)
    execu.complete_job(sim, job)

    assert execu.used_core_hours == (6 * 1 * 120.0) / 3600
    assert execu.used_gpu_hours == (3 * 1 * 120.0) / 3600

    broken = SimpleExec(num_nodes=1, cores_per_node=2, gpus_per_node=1, exclusive=False)
    broken.post_analysis(SimpleNamespace(current_time=0.0))
    assert "Makespan beginning" in caplog.text
    assert "Total core hours is 0." in caplog.text
    assert "Total GPU hours is 0." in caplog.text
    output = capsys.readouterr().out
    assert "Used Core-Hours:" in output
