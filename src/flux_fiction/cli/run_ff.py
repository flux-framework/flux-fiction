#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import datetime
import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
import hashlib
from typing import Any


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def workspace_root() -> Path:
    override = os.environ.get("FLUX_FICTION_WORKSPACE_ROOT")
    if override:
        return Path(override).expanduser().resolve()
    return repo_root().parent


def default_flux_prefix() -> Path:
    return workspace_root() / "container-installs" / "flux-core"


def legacy_prefixes() -> list[tuple[Path, Path]]:
    ff_root = repo_root()
    ws_root = workspace_root()
    return [
        (Path("/home/j/Desktop/flux/sc25_poster/flux-fiction"), ff_root),
        (Path("/home/j/Desktop/flux/sc25_poster"), ws_root),
        (Path("/work/flux/sc25_poster/flux-fiction"), ff_root),
        (Path("/work/flux/sc25_poster"), ws_root),
    ]


def path_mappings() -> list[tuple[Path, Path]]:
    mappings: list[tuple[Path, Path]] = []
    raw = os.environ.get("FLUX_FICTION_PATH_MAP", "")
    for entry in raw.split(os.pathsep):
        entry = entry.strip()
        if not entry:
            continue
        if "=" in entry:
            src, dst = entry.split("=", 1)
        elif ">" in entry:
            src, dst = entry.split(">", 1)
        else:
            raise ValueError(
                "FLUX_FICTION_PATH_MAP entries must look like '/host=/container'"
            )
        mappings.append((Path(src).expanduser(), Path(dst).expanduser()))

    mappings.extend(legacy_prefixes())

    return sorted(dict.fromkeys(mappings), key=lambda item: len(str(item[0])), reverse=True)


def remap_path(path: str | Path, *, for_output: bool = False) -> Path:
    p = Path(path).expanduser()
    if not p.is_absolute():
        p = p.resolve()
    if p.exists():
        return p

    raw = str(p)
    for src, dst in path_mappings():
        src_s = str(src).rstrip("/")
        if raw != src_s and not raw.startswith(src_s + "/"):
            continue
        suffix = raw[len(src_s):].lstrip("/")
        candidate = dst / suffix if suffix else dst
        if for_output or candidate.exists():
            return candidate
    return p


def load_toml(path: Path) -> dict[str, Any]:
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore

    with path.open("rb") as f:
        return tomllib.load(f)


def toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if value is None:
        return '""'
    return json.dumps(str(value))


def write_flux_fiction_toml(path: Path, data: dict[str, Any]) -> None:
    lines = ["[flux_fiction]"]
    for key, value in data.items():
        lines.append(f"{key} = {toml_value(value)}")
    path.write_text("\n".join(lines) + "\n")


def drop_faketime_env(env: dict[str, str]) -> dict[str, str]:
    cleaned = dict(env)
    for key in list(cleaned):
        if key == "LD_PRELOAD" or key.startswith("FAKETIME_"):
            cleaned.pop(key, None)
    return cleaned


def broker_log_matches(log_path: Path, needle: str) -> list[str]:
    if not log_path.exists():
        return []

    matches: list[str] = []
    with log_path.open("r", encoding="utf-8", errors="replace") as f:
        for lineno, line in enumerate(f, start=1):
            if needle in line:
                matches.append(f"{lineno}: {line.rstrip()}")
    return matches


SCHED_RESOURCE_ERROR_NEEDLE = "sched-fluxion-resource.err"


def _terminate_process_group(proc: subprocess.Popen[str], sig: int) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, sig)
    except ProcessLookupError:
        return


def _watch_broker_log_for_fatal_error(
    broker_log: Path,
    proc: subprocess.Popen[str],
    detected: dict[str, str],
    run_root: Path,
) -> None:
    offset = 0
    last_match = ""

    while proc.poll() is None:
        if broker_log.exists():
            with broker_log.open("r", encoding="utf-8", errors="replace") as f:
                f.seek(offset)
                while True:
                    line = f.readline()
                    if not line:
                        break
                    if SCHED_RESOURCE_ERROR_NEEDLE in line:
                        last_match = line.rstrip()
                        detected["message"] = (
                            "Detected sched-fluxion-resource.err in broker log; "
                            "terminating Flux instance.\n{}".format(last_match)
                        )
                        break
                offset = f.tell()

        if last_match:
            print(detected["message"], file=sys.stderr, flush=True)
            sentinel = run_root / ".fatal_scheduler_error"
            try:
                sentinel.write_text(detected["message"] + "\n")
            except Exception:
                print(
                    f"Failed to write fatal-error sentinel at {sentinel}",
                    file=sys.stderr,
                    flush=True,
                )
            grace_s = float(os.environ.get("FLUX_FICTION_FATAL_ERROR_GRACE_SECONDS", "10"))
            output_dir = run_root / "output"
            deadline = time.time() + max(0.0, grace_s)
            while proc.poll() is None and time.time() < deadline:
                done_marker = output_dir / ".emergency_dump_done"
                if done_marker.exists():
                    print(
                        "Fatal scheduler error detected; emergency diagnostics were written.",
                        file=sys.stderr,
                        flush=True,
                    )
                    break
                time.sleep(0.2)
            _terminate_process_group(proc, signal.SIGTERM)

            deadline = time.time() + 10.0
            while proc.poll() is None and time.time() < deadline:
                time.sleep(0.1)

            if proc.poll() is None:
                print(
                    "Flux instance did not exit after SIGTERM; sending SIGKILL.",
                    file=sys.stderr,
                    flush=True,
                )
                _terminate_process_group(proc, signal.SIGKILL)
            return

        time.sleep(0.2)


def first_submit_epoch(trace_path: Path) -> float:
    with trace_path.open(newline="") as f:
        reader = csv.DictReader(line for line in f if not line.startswith("#"))
        for row in reader:
            raw = row.get("t_submit")
            if raw not in (None, ""):
                return float(raw)

            submit = row.get("Submit")
            if submit:
                dt = datetime.fromisoformat(submit)
                epoch = datetime(1970, 1, 1)
                return (dt - epoch).total_seconds()

            raise ValueError(f"Cannot find Submit or t_submit in first trace row: {row}")

    raise ValueError(f"Trace file is empty: {trace_path}")


def make_run_id(config_path: Path, tag: str | None) -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pieces = [stamp, config_path.stem]
    if tag:
        pieces.append(tag)
    return "_".join(piece.replace("/", "_") for piece in pieces)


def unique_run_root(config_path: Path, run_dir: str | Path | None, tag: str | None) -> Path:
    if run_dir:
        base = remap_path(run_dir, for_output=True)
    else:
        base = config_path.parent / "runs" / make_run_id(config_path, tag)

    candidate = base
    idx = 2
    while candidate.exists():
        candidate = Path(f"{base}_{idx}")
        idx += 1
    return candidate


def shell_quote(value: str | Path) -> str:
    return shlex.quote(str(value))


def default_stampfile() -> str:
    env_stamp = os.environ.get("STAMPFILE")
    if env_stamp:
        return env_stamp
    if Path("/host-tmp").exists():
        return "/host-tmp/faketime_stamp"
    return "/tmp/flux_fiction_faketime_stamp"


def default_faketime_lib() -> str:
    env_lib = os.environ.get("FAKETIME_LIB")
    if env_lib:
        return env_lib

    candidates = [
        "/usr/lib/x86_64-linux-gnu/faketime/libfaketimeMT.so.1",
        "/usr/lib/aarch64-linux-gnu/faketime/libfaketimeMT.so.1",
        "/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1",
        "/usr/lib/aarch64-linux-gnu/faketime/libfaketime.so.1",
    ]
    for candidate in candidates:
        if Path(candidate).exists():
            return candidate

    return candidates[0]


def configure_flux_env(env: dict[str, str]) -> None:
    prefix = Path(env.get("FLUX_PREFIX", str(default_flux_prefix())))
    if not prefix.exists():
        return

    env["FLUX_PREFIX"] = str(prefix)
    env["PATH"] = f"{prefix / 'bin'}:{env.get('PATH', '')}"
    env["LD_LIBRARY_PATH"] = (
        f"{prefix / 'lib'}:{prefix / 'lib64'}:{env.get('LD_LIBRARY_PATH', '')}"
    )
    env["PKG_CONFIG_PATH"] = (
        f"{prefix / 'lib' / 'pkgconfig'}:"
        f"{prefix / 'lib64' / 'pkgconfig'}:"
        f"{env.get('PKG_CONFIG_PATH', '')}"
    )


def prepare_config(
    source_config: Path,
    run_root: Path,
    *,
    otel: dict[str, str | bool] | None = None,
) -> tuple[Path, Path, Path]:
    cfg_doc = load_toml(source_config)
    cfg = dict(cfg_doc.get("flux_fiction", cfg_doc))

    output_dir = run_root / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    for key in ("job_traces", "resource_file", "resource_R"):
        if cfg.get(key):
            mapped = remap_path(cfg[key])
            if key == "job_traces" and not mapped.exists():
                raise FileNotFoundError(f"job_traces not found after path mapping: {mapped}")
            if key in {"resource_file", "resource_R"} and not mapped.exists():
                raise FileNotFoundError(f"{key} not found after path mapping: {mapped}")
            cfg[key] = str(mapped)

    if cfg.get("config_json"):
        source_json = remap_path(cfg["config_json"])
        if not source_json.exists():
            raise FileNotFoundError(f"config_json not found after path mapping: {source_json}")
        copied_json = run_root / source_json.name
        shutil.copy2(source_json, copied_json)
        cfg["config_json"] = str(copied_json)

    cfg["output_dir"] = str(output_dir) + "/"
    cfg["log_file"] = str(run_root / "emu.log")
    if otel:
        cfg.update(otel)

    generated_config = run_root / source_config.name
    write_flux_fiction_toml(generated_config, cfg)
    return generated_config, remap_path(cfg["job_traces"]), output_dir


def build_inner_script(ff_root: Path, generated_config: Path, stampfile: str | None) -> str:
    run_root = generated_config.parent
    output_dir = run_root / "output"
    fatal_sentinel = run_root / ".fatal_scheduler_error"
    emergency_done = output_dir / ".emergency_dump_done"
    emergency_eventlogs = output_dir / "emergency_eventlogs.txt"
    emergency_alloc_json = output_dir / "emergency_allocations.json"
    emergency_alloc_txt = output_dir / "emergency_allocations.txt"
    emergency_jobspecs = output_dir / "emergency_jobspecs.txt"
    emergency_flux_config = output_dir / "emergency_flux_config.json"
    sample_jobs_path = output_dir / "sample_jobs.json"
    sample_jobid_path = output_dir / "sample_jobid.txt"
    sample_jobspec_path = output_dir / "sample_jobspec.json"
    args = [
        "flux-fiction",
        "--config_file",
        str(generated_config),
    ]
    if stampfile:
        args.extend([
            "--faketime_timestamp_file",
            stampfile,
            "--faketime_no_seed",
            "--faketime_tolerance",
            "0.000001",
            "--faketime_near_event_threshold",
            "0",
        ])

    python_dump = """
import json
import os
import subprocess
import time
from pathlib import Path

jobs_path = Path({jobs_path!r})
eventlog_path = Path({eventlog_path!r})
alloc_path = Path({alloc_path!r})
jobspec_path = Path({jobspec_path!r})
flux_config_path = Path({flux_config_path!r})
done_path = Path({done_path!r})

COMMAND_TIMEOUT = float(os.environ.get("FLUX_FICTION_EMERGENCY_DUMP_TIMEOUT_SECONDS", "3"))
TOTAL_BUDGET = float(os.environ.get("FLUX_FICTION_EMERGENCY_DUMP_BUDGET_SECONDS", "8"))
JOB_DETAIL_LIMIT = int(os.environ.get("FLUX_FICTION_EMERGENCY_JOB_LIMIT", "20"))
deadline = time.monotonic() + max(1.0, TOTAL_BUDGET)


def remaining_budget() -> float:
    return deadline - time.monotonic()


def run_capture(args):
    remaining = remaining_budget()
    if remaining <= 0:
        return subprocess.CompletedProcess(
            args=args,
            returncode=124,
            stdout="",
            stderr="[skipped: emergency dump time budget exceeded]\\n",
        )
    timeout = min(COMMAND_TIMEOUT, max(0.25, remaining))
    try:
        return subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        stderr = exc.stderr or ""
        if stderr and not stderr.endswith("\\n"):
            stderr += "\\n"
        stderr += f"[timed out after {{timeout:.2f}}s]\\n"
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
            stderr=f"[exception] {{type(exc).__name__}}: {{exc}}\\n",
        )


for path in (jobs_path, eventlog_path, alloc_path, jobspec_path, flux_config_path):
    path.write_text("[emergency dump started]\\n")

jobs_payload = "{{}}"
jobs = []
jobs_proc = run_capture(["flux", "jobs", "-a", "--json"])
if jobs_proc.stdout.strip():
    jobs_payload = jobs_proc.stdout
jobs_path.write_text(jobs_payload)

if jobs_proc.returncode == 0:
    try:
        jobs = json.loads(jobs_payload).get("jobs", [])
    except Exception:
        jobs = []

flux_config_proc = run_capture(["bash", "-lc", "flux config get | jq"])
if flux_config_proc.stdout.strip():
    flux_config_path.write_text(flux_config_proc.stdout)
else:
    lines = ["flux config get | jq produced no stdout\\n"]
    if flux_config_proc.stderr:
        lines.extend(["[stderr]\\n", flux_config_proc.stderr])
    flux_config_path.write_text("".join(lines))
if flux_config_proc.returncode != 0 and flux_config_proc.stderr:
    with flux_config_path.open("a") as cf:
        if flux_config_proc.stdout.strip():
            cf.write("\\n")
        cf.write("[stderr]\\n" + flux_config_proc.stderr)

with eventlog_path.open("w") as ef, alloc_path.open("w") as af, jobspec_path.open("w") as jf:
    detailed_jobs = jobs[:JOB_DETAIL_LIMIT]
    if len(jobs) > len(detailed_jobs):
        note = (
            f"[truncated detailed job dumps at {{len(detailed_jobs)}} of {{len(jobs)}} jobs; "
            "full job list is in emergency_allocations.json]\\n"
        )
        ef.write(note)
        af.write(note)
        jf.write(note)
    if jobs_proc.returncode != 0:
        ef.write("flux jobs --json failed\\n")
        ef.write(jobs_proc.stderr)
        af.write("flux jobs --json failed\\n")
        af.write(jobs_proc.stderr)
        jf.write("flux jobs --json failed\\n")
        jf.write(jobs_proc.stderr)
    for job in detailed_jobs:
        if remaining_budget() <= 0:
            budget_note = "[stopped collecting per-job diagnostics: emergency dump time budget exceeded]\\n"
            ef.write(budget_note)
            af.write(budget_note)
            jf.write(budget_note)
            break
        jobid = str(job.get("jobid") or job.get("id") or "")
        ef.write(f"=== {{jobid}} ===\\n")
        eventlog_proc = run_capture(["flux", "job", "eventlog", jobid])
        ef.write(eventlog_proc.stdout)
        if eventlog_proc.stderr:
            ef.write("\\n[stderr]\\n" + eventlog_proc.stderr)
        ef.write("\\n")

        af.write(f"=== {{jobid}} ===\\n")
        af.write(json.dumps(job, sort_keys=True))
        af.write("\\nR:\\n")
        alloc_proc = run_capture(["flux", "job", "info", jobid, "R"])
        af.write(alloc_proc.stdout)
        if alloc_proc.stderr:
            af.write("\\n[stderr]\\n" + alloc_proc.stderr)
        af.write("\\n")

        jobspec_proc = run_capture(["flux", "job", "info", jobid, "jobspec"])
        if jobspec_proc.stdout.strip():
            jf.write(jobspec_proc.stdout.rstrip() + "\\n")
        else:
            jf.write(f"=== {{jobid}} ===\\n")
            jf.write("[jobspec missing]\\n")
        if jobspec_proc.stderr:
            jf.write("[stderr]\\n" + jobspec_proc.stderr)
        if not jobspec_proc.stdout.endswith("\\n"):
            jf.write("\\n")
done_path.write_text("ok\\n")
""".format(
        jobs_path=str(emergency_alloc_json),
        eventlog_path=str(emergency_eventlogs),
        alloc_path=str(emergency_alloc_txt),
        jobspec_path=str(emergency_jobspecs),
        flux_config_path=str(emergency_flux_config),
        done_path=str(emergency_done),
    )

    python_capture_sample_jobspec = """
import json
import subprocess
from pathlib import Path

jobs_path = Path({jobs_path!r})
jobid_path = Path({jobid_path!r})
jobspec_path = Path({jobspec_path!r})

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

jobid_path.write_text(jobid + "\\n", encoding="utf-8")
jobspec_proc = subprocess.run(
    ["flux", "job", "info", jobid, "jobspec"],
    capture_output=True,
    text=True,
)
if jobspec_proc.returncode != 0:
    raise SystemExit(jobspec_proc.stderr or f"flux job info {{jobid}} jobspec failed")

jobspec_path.write_text(jobspec_proc.stdout, encoding="utf-8")
print(jobid)
""".format(
        jobs_path=str(sample_jobs_path),
        jobid_path=str(sample_jobid_path),
        jobspec_path=str(sample_jobspec_path),
    )

    return "\n".join([
        "set -euo pipefail",
        f"cd {shell_quote(ff_root)}",
        "source ./load_jobtap.sh",
        f"fatal_sentinel={shell_quote(fatal_sentinel)}",
        f"emergency_done={shell_quote(emergency_done)}",
        "emergency_dump() {",
        "  if [[ -f \"${emergency_done}\" ]]; then",
        "    return 0",
        "  fi",
        "  python3 - <<'PY'",
        python_dump.strip(),
        "PY",
        "}",
        "watch_fatal_sentinel() {",
        "  while kill -0 \"${ff_pid}\" 2>/dev/null; do",
        "    if [[ -f \"${fatal_sentinel}\" ]]; then",
        "      emergency_dump || true",
        "      kill -TERM \"${ff_pid}\" 2>/dev/null || true",
        "      return 0",
        "    fi",
        "    sleep 0.2",
        "  done",
        "}",
        " ".join(shell_quote(part) for part in args) + " &",
        "ff_pid=$!",
        "watch_fatal_sentinel &",
        "fatal_watch_pid=$!",
        "set +e",
        "wait \"${ff_pid}\"",
        "ff_rc=$?",
        "set -e",
        "kill \"${fatal_watch_pid}\" 2>/dev/null || true",
        "wait \"${fatal_watch_pid}\" 2>/dev/null || true",
        "if [[ -f \"${fatal_sentinel}\" ]]; then",
        "  emergency_dump || true",
        "fi",
        "if [[ \"${ff_rc}\" -eq 0 ]]; then",
        "  python3 - <<'PY'",
        python_capture_sample_jobspec.strip(),
        "PY",
        "fi",
        "exit \"${ff_rc}\"",
    ])


def write_inner_script(path: Path, script: str) -> None:
    path.write_text(script + "\n")
    path.chmod(0o755)


def write_reproducer(
    path: Path,
    cmd: list[str],
    env_updates: dict[str, str],
    extra_lines: list[str] | None = None,
) -> None:
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "",
    ]
    for key, value in env_updates.items():
        lines.append(f"export {key}={shell_quote(value)}")
    if extra_lines:
        lines.extend(["", *extra_lines])
    lines.extend([
        "",
        " ".join(shell_quote(part) for part in cmd),
        "",
    ])
    path.write_text("\n".join(lines))
    path.chmod(0o755)


def build_bridge_command(
    ff_root: Path,
    socket_path: Path,
    endpoint: str,
    service_name: str,
    summary_file: Path,
    spans_file: Path,
    log_file: Path,
) -> list[str]:
    return [
        sys.executable,
        "-m",
        "flux_fiction.telemetry.bridge",
        "--socket",
        str(socket_path),
        "--endpoint",
        endpoint,
        "--service-name",
        service_name,
        "--summary-file",
        str(summary_file),
        "--spans-file",
        str(spans_file),
        "--log-file",
        str(log_file),
    ]


def make_otel_socket_path(run_root: Path) -> Path:
    digest = hashlib.sha1(str(run_root).encode("utf-8")).hexdigest()[:12]
    return Path("/tmp") / f"flux-fiction-otel-{digest}.sock"


def ensure_otel_dependencies() -> None:
    required = [
        "opentelemetry",
        "opentelemetry.sdk",
        "opentelemetry.exporter.otlp.proto.http.trace_exporter",
    ]
    missing = [name for name in required if importlib.util.find_spec(name) is None]
    if missing:
        raise RuntimeError(
            "OpenTelemetry profiling requested, but required Python modules are missing: {}"
            .format(", ".join(missing))
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Flux Fiction in a fresh Flux instance with jobtap and libfaketime configured."
    )
    parser.add_argument("config_file", help="Flux Fiction TOML config file.")
    parser.add_argument("--tag", help="Optional suffix for the timestamped run directory.")
    parser.add_argument(
        "--run-dir",
        help="Explicit run directory. Default: <config_dir>/runs/<timestamp>_<config_stem>.",
    )
    parser.add_argument(
        "--faketime-lib",
        default=default_faketime_lib(),
        help="Path to libfaketimeMT.so.1.",
    )
    parser.add_argument(
        "--stampfile",
        default=default_stampfile(),
        help="Path to the libfaketime timestamp file.",
    )
    parser.add_argument(
        "--no-faketime",
        action="store_true",
        help="Run without LD_PRELOAD/libfaketime.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Prepare the run directory and print the command without launching Flux.",
    )
    parser.add_argument(
        "--broker-log-level",
        type=int,
        default=6,
        help=(
            "Flux broker log-level attribute. Default 6 keeps info and above; "
            "use 7 for debug."
        ),
    )
    parser.add_argument(
        "--faketime-start-lead",
        type=float,
        default=30.0,
        help=(
            "Seconds before the first trace submit time used to seed libfaketime. "
            "This prevents Flux startup from drifting past the first submit before "
            "Flux Fiction begins."
        ),
    )
    parser.add_argument(
        "--otel",
        action="store_true",
        help="Enable out-of-process OpenTelemetry profiling for the simulator and jobtap plugin.",
    )
    parser.add_argument(
        "--otel-endpoint",
        default=os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://127.0.0.1:4318/v1/traces"),
        help="OTLP HTTP traces endpoint used by the profiling bridge.",
    )
    parser.add_argument(
        "--otel-service-name",
        default="flux-fiction",
        help="Base OpenTelemetry service name written by the profiling bridge.",
    )
    args = parser.parse_args()
    if args.otel:
        ensure_otel_dependencies()

    ff_root = repo_root()
    source_config = remap_path(args.config_file)
    if not source_config.exists():
        raise FileNotFoundError(f"Config TOML not found: {source_config}")

    run_root = unique_run_root(source_config, args.run_dir, args.tag)
    run_root.mkdir(parents=True, exist_ok=False)
    otel_socket = make_otel_socket_path(run_root)
    otel_summary = run_root / "otel_summary.csv"
    otel_spans = run_root / "otel_spans.jsonl"
    otel_log = run_root / "otel_bridge.log"
    otel_cfg = None
    if args.otel:
        otel_cfg = {
            "otel_enabled": True,
            "otel_endpoint": args.otel_endpoint,
            "otel_service_name": args.otel_service_name,
            "otel_bridge_socket": str(otel_socket),
            "otel_summary_file": str(otel_summary),
            "otel_spans_file": str(otel_spans),
            "otel_bridge_log_file": str(otel_log),
        }
    generated_config, trace_path, output_dir = prepare_config(
        source_config,
        run_root,
        otel=otel_cfg,
    )

    broker_log = run_root / "broker.log"
    stdout_log = run_root / "run.log"
    stampfile = None if args.no_faketime else args.stampfile

    env = os.environ.copy()
    configure_flux_env(env)
    env.setdefault(
        "FLUX_FICTION_PATH_MAP",
        f"/home/j/Desktop/flux/sc25_poster={workspace_root()}",
    )

    faketime_env: dict[str, str] = {}
    first_submit = None
    if not args.no_faketime:
        faketime_lib = remap_path(args.faketime_lib)
        if not args.dry_run and not faketime_lib.exists():
            raise FileNotFoundError(f"libfaketime not found: {faketime_lib}")

        stamp_path = Path(args.stampfile)
        first_submit = first_submit_epoch(trace_path)
        initial_fake_time = first_submit - max(0.0, float(args.faketime_start_lead))
        if not args.dry_run:
            stamp_path.parent.mkdir(parents=True, exist_ok=True)
            stamp_path.write_text(f"{initial_fake_time - time.time():+.9f}s\n")

        faketime_env = {
            "FLUX_LOAD_WITH_DEEPBIND": "0",
            "LD_PRELOAD": str(faketime_lib),
            "FAKETIME_TIMESTAMP_FILE": str(stamp_path),
            "FAKETIME_NO_CACHE": "1",
        }
        env.update(faketime_env)

    inner_script = build_inner_script(ff_root, generated_config, stampfile)
    inner_script_path = run_root / "run_inner.sh"
    write_inner_script(inner_script_path, inner_script)
    flux_exe = shutil.which("flux", path=env.get("PATH")) or "flux"
    cmd = [
        flux_exe,
        "start",
        f"--setattr=log-filename={broker_log}",
        f"--setattr=log-level={args.broker_log_level}",
        "--",
        "bash",
        str(inner_script_path),
    ]

    bridge_cmd = None
    bridge_extra_lines: list[str] = []
    if args.otel:
        bridge_cmd = build_bridge_command(
            ff_root=ff_root,
            socket_path=otel_socket,
            endpoint=args.otel_endpoint,
            service_name=args.otel_service_name,
            summary_file=otel_summary,
            spans_file=otel_spans,
            log_file=otel_log,
        )
        bridge_extra_lines = [
            "unset LD_PRELOAD",
            "unset FAKETIME_TIMESTAMP_FILE",
            "unset FAKETIME_NO_CACHE",
            " ".join(shell_quote(part) for part in bridge_cmd) + " &",
            "bridge_pid=$!",
            "trap 'kill ${bridge_pid} 2>/dev/null || true; wait ${bridge_pid} 2>/dev/null || true' EXIT",
        ]

    write_reproducer(
        run_root / "reproduce.sh",
        cmd,
        {
            "FLUX_FICTION_PATH_MAP": env["FLUX_FICTION_PATH_MAP"],
            **faketime_env,
        },
        extra_lines=bridge_extra_lines,
    )

    print(f"Source config:    {source_config}")
    print(f"Generated config: {generated_config}")
    print(f"Output dir:       {output_dir}")
    print(f"Broker log:       {broker_log}")
    print(f"Run log:          {stdout_log}")
    if not args.no_faketime:
        print(f"Stamp file:       {args.stampfile}")
        print(f"First submit:     {first_submit:.6f}")
        print(f"Faketime lead:    {max(0.0, float(args.faketime_start_lead)):.6f}s")
    if args.otel:
        print(f"OTel socket:      {otel_socket}")
        print(f"OTel summary:     {otel_summary}")
        print(f"OTel spans:       {otel_spans}")
        print(f"OTel bridge log:  {otel_log}")
    print(f"Reproducer:       {run_root / 'reproduce.sh'}")
    print("")
    print("Command:")
    print(" ".join(shell_quote(part) for part in cmd))
    sys.stdout.flush()

    if args.dry_run:
        print("\nDry run requested; not launching Flux.")
        return 0

    bridge_proc = None
    proc = None
    broker_watch = None
    detected_fatal_error: dict[str, str] = {}
    try:
        if bridge_cmd is not None:
            bridge_env = drop_faketime_env(env)
            bridge_proc = subprocess.Popen(
                bridge_cmd,
                cwd=ff_root,
                env=bridge_env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            for _ in range(50):
                if otel_socket.exists():
                    break
                if bridge_proc.poll() is not None:
                    raise RuntimeError(
                        "OpenTelemetry bridge exited early; see {}".format(otel_log)
                    )
                time.sleep(0.1)
            else:
                raise RuntimeError(
                    "Timed out waiting for OpenTelemetry bridge socket at {}".format(otel_socket)
                )

        with stdout_log.open("w") as f:
            proc = subprocess.Popen(
                cmd,
                cwd=ff_root,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                errors="replace",
                start_new_session=True,
            )
            broker_watch = threading.Thread(
                target=_watch_broker_log_for_fatal_error,
                args=(broker_log, proc, detected_fatal_error, run_root),
                daemon=True,
            )
            broker_watch.start()
            assert proc.stdout is not None
            for line in proc.stdout:
                f.write(line)
                f.flush()
                sys.stdout.write(line)
            proc.wait()
    finally:
        if broker_watch is not None:
            broker_watch.join(timeout=1)
        if bridge_proc is not None:
            bridge_proc.terminate()
            try:
                bridge_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                bridge_proc.kill()
                bridge_proc.wait(timeout=5)

    if proc is None:
        return 1

    broker_errors = broker_log_matches(broker_log, SCHED_RESOURCE_ERROR_NEEDLE)
    emergency_dir = run_root / "output"
    emergency_done = emergency_dir / ".emergency_dump_done"
    emergency_files = [
        emergency_dir / "emergency_eventlogs.txt",
        emergency_dir / "emergency_allocations.json",
        emergency_dir / "emergency_allocations.txt",
        emergency_dir / "emergency_jobspecs.txt",
        emergency_dir / "emergency_flux_config.json",
    ]
    if detected_fatal_error or broker_errors:
        failure_path = run_root / "harness_failure.txt"
        failure_lines = []
        if detected_fatal_error:
            failure_lines.append(detected_fatal_error["message"])
        if broker_errors:
            failure_lines.append(f"Detected {len(broker_errors)} broker log matches for {SCHED_RESOURCE_ERROR_NEEDLE}.")
            failure_lines.extend(broker_errors[:20])
        failure_path.write_text("\n".join(failure_lines) + "\n")
        print(f"Harness failure report: {failure_path}", file=sys.stderr)
    if emergency_done.exists():
        print("Emergency diagnostics written:", file=sys.stderr)
        for path in emergency_files:
            if path.exists():
                print(f"  {path}", file=sys.stderr)
    if proc.returncode != 0:
        print(f"Run failed with rc={proc.returncode}. See {stdout_log}", file=sys.stderr)
    if broker_errors:
        print(
            (
                "Run failed: detected sched-fluxion-resource.err in "
                f"{broker_log}"
            ),
            file=sys.stderr,
        )
        for line in broker_errors[:5]:
            print(f"  {line}", file=sys.stderr)
        if len(broker_errors) > 5:
            print(f"  ... {len(broker_errors) - 5} more matching lines", file=sys.stderr)

    return proc.returncode or (1 if broker_errors else 0)


if __name__ == "__main__":
    raise SystemExit(main())
