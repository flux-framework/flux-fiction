from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import time


def _run_capture(args: list[str], *, deadline: float, command_timeout: float) -> subprocess.CompletedProcess[str]:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return subprocess.CompletedProcess(
            args=args,
            returncode=124,
            stdout="",
            stderr="[skipped: emergency dump time budget exceeded]\n",
        )

    timeout = min(command_timeout, max(0.25, remaining))
    try:
        return subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        stderr = exc.stderr or ""
        if stderr and not stderr.endswith("\n"):
            stderr += "\n"
        stderr += f"[timed out after {timeout:.2f}s]\n"
        return subprocess.CompletedProcess(
            args=args,
            returncode=124,
            stdout=exc.stdout or "",
            stderr=stderr,
        )
    except Exception as exc:
        return subprocess.CompletedProcess(
            args=args,
            returncode=125,
            stdout="",
            stderr=f"[exception] {type(exc).__name__}: {exc}\n",
        )


def emergency_dump(args: argparse.Namespace) -> int:
    jobs_path = Path(args.jobs_path)
    eventlog_path = Path(args.eventlog_path)
    alloc_path = Path(args.alloc_path)
    jobspec_path = Path(args.jobspec_path)
    flux_config_path = Path(args.flux_config_path)
    done_path = Path(args.done_path)

    command_timeout = float(os.environ.get("FLUX_FICTION_EMERGENCY_DUMP_TIMEOUT_SECONDS", "3"))
    total_budget = float(os.environ.get("FLUX_FICTION_EMERGENCY_DUMP_BUDGET_SECONDS", "8"))
    job_detail_limit = int(os.environ.get("FLUX_FICTION_EMERGENCY_JOB_LIMIT", "20"))
    deadline = time.monotonic() + max(1.0, total_budget)

    for path in (jobs_path, eventlog_path, alloc_path, jobspec_path, flux_config_path):
        path.write_text("[emergency dump started]\n", encoding="utf-8")

    jobs_payload = "{}"
    jobs = []
    jobs_proc = _run_capture(["flux", "jobs", "-a", "--json"], deadline=deadline, command_timeout=command_timeout)
    if jobs_proc.stdout.strip():
        jobs_payload = jobs_proc.stdout
    jobs_path.write_text(jobs_payload, encoding="utf-8")

    if jobs_proc.returncode == 0:
        try:
            jobs = json.loads(jobs_payload).get("jobs", [])
        except Exception:
            jobs = []

    flux_config_proc = _run_capture(["bash", "-lc", "flux config get | jq"], deadline=deadline, command_timeout=command_timeout)
    if flux_config_proc.stdout.strip():
        flux_config_path.write_text(flux_config_proc.stdout, encoding="utf-8")
    else:
        lines = ["flux config get | jq produced no stdout\n"]
        if flux_config_proc.stderr:
            lines.extend(["[stderr]\n", flux_config_proc.stderr])
        flux_config_path.write_text("".join(lines), encoding="utf-8")
    if flux_config_proc.returncode != 0 and flux_config_proc.stderr:
        with flux_config_path.open("a", encoding="utf-8") as cf:
            if flux_config_proc.stdout.strip():
                cf.write("\n")
            cf.write("[stderr]\n" + flux_config_proc.stderr)

    with (
        eventlog_path.open("w", encoding="utf-8") as ef,
        alloc_path.open("w", encoding="utf-8") as af,
        jobspec_path.open("w", encoding="utf-8") as jf,
    ):
        detailed_jobs = jobs[:job_detail_limit]
        if len(jobs) > len(detailed_jobs):
            note = (
                f"[truncated detailed job dumps at {len(detailed_jobs)} of {len(jobs)} jobs; "
                "full job list is in emergency_allocations.json]\n"
            )
            ef.write(note)
            af.write(note)
            jf.write(note)
        if jobs_proc.returncode != 0:
            ef.write("flux jobs --json failed\n")
            ef.write(jobs_proc.stderr)
            af.write("flux jobs --json failed\n")
            af.write(jobs_proc.stderr)
            jf.write("flux jobs --json failed\n")
            jf.write(jobs_proc.stderr)

        for job in detailed_jobs:
            if deadline - time.monotonic() <= 0:
                budget_note = "[stopped collecting per-job diagnostics: emergency dump time budget exceeded]\n"
                ef.write(budget_note)
                af.write(budget_note)
                jf.write(budget_note)
                break

            jobid = str(job.get("jobid") or job.get("id") or "")

            ef.write(f"=== {jobid} ===\n")
            eventlog_proc = _run_capture(["flux", "job", "eventlog", jobid], deadline=deadline, command_timeout=command_timeout)
            ef.write(eventlog_proc.stdout)
            if eventlog_proc.stderr:
                ef.write("\n[stderr]\n" + eventlog_proc.stderr)
            ef.write("\n")

            af.write(f"=== {jobid} ===\n")
            af.write(json.dumps(job, sort_keys=True))
            af.write("\nR:\n")
            alloc_proc = _run_capture(["flux", "job", "info", jobid, "R"], deadline=deadline, command_timeout=command_timeout)
            af.write(alloc_proc.stdout)
            if alloc_proc.stderr:
                af.write("\n[stderr]\n" + alloc_proc.stderr)
            af.write("\n")

            jobspec_proc = _run_capture(["flux", "job", "info", jobid, "jobspec"], deadline=deadline, command_timeout=command_timeout)
            if jobspec_proc.stdout.strip():
                jf.write(jobspec_proc.stdout.rstrip() + "\n")
            else:
                jf.write(f"=== {jobid} ===\n")
                jf.write("[jobspec missing]\n")
            if jobspec_proc.stderr:
                jf.write("[stderr]\n" + jobspec_proc.stderr)
            if not jobspec_proc.stdout.endswith("\n"):
                jf.write("\n")

    done_path.write_text("ok\n", encoding="utf-8")
    return 0


def capture_sample_jobspec(args: argparse.Namespace) -> int:
    jobs_path = Path(args.jobs_path)
    jobid_path = Path(args.jobid_path)
    jobspec_path = Path(args.jobspec_path)

    jobs_proc = subprocess.run(
        ["flux", "jobs", "-a", "--json"],
        capture_output=True,
        text=True,
    )
    if jobs_proc.returncode != 0:
        raise SystemExit(jobs_proc.stderr or "flux jobs --json failed")

    jobs_path.write_text(jobs_proc.stdout, encoding="utf-8")
    payload = json.loads(jobs_proc.stdout)
    jobs = payload.get("jobs", [])
    if not jobs:
        raise SystemExit("No jobs found in flux jobs --json output")

    job = jobs[0]
    jobid = str(job.get("jobid") or job.get("id") or "")
    if not jobid:
        raise SystemExit("Could not determine jobid from flux jobs --json output")

    jobid_path.write_text(jobid + "\n", encoding="utf-8")
    jobspec_proc = subprocess.run(
        ["flux", "job", "info", jobid, "jobspec"],
        capture_output=True,
        text=True,
    )
    if jobspec_proc.returncode != 0:
        raise SystemExit(jobspec_proc.stderr or f"flux job info {jobid} jobspec failed")

    jobspec_path.write_text(jobspec_proc.stdout, encoding="utf-8")
    print(jobid)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Flux Fiction broker-side diagnostics helpers.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    dump_parser = subparsers.add_parser("emergency-dump")
    dump_parser.add_argument("--jobs-path", required=True)
    dump_parser.add_argument("--eventlog-path", required=True)
    dump_parser.add_argument("--alloc-path", required=True)
    dump_parser.add_argument("--jobspec-path", required=True)
    dump_parser.add_argument("--flux-config-path", required=True)
    dump_parser.add_argument("--done-path", required=True)
    dump_parser.set_defaults(func=emergency_dump)

    sample_parser = subparsers.add_parser("capture-sample-jobspec")
    sample_parser.add_argument("--jobs-path", required=True)
    sample_parser.add_argument("--jobid-path", required=True)
    sample_parser.add_argument("--jobspec-path", required=True)
    sample_parser.set_defaults(func=capture_sample_jobspec)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
