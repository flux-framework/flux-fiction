"""
Legacy command-line shim for Flux Fiction.

Keep new integrations on the API layer in `flux_fiction.api.client`.
This module exists for backward-compatible CLI usage and should remain thin.
"""

from __future__ import annotations

import argparse

from flux_fiction.api import client

import logging
logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument("--job_traces",      type=str, default=None, help="Path to historical job data/traces. Can be defined in config file.")
    parser.add_argument("--config_file",     type=str, default=None, help="Path to configuration TOML (optional but recommended). Do not use alongside the JSON")
    parser.add_argument("--config_json",     type=str, default=None, help="Path to configuration JSON (optional but recommended). Do not use alongside the TOML. This was added because flux config get outputs a json, and it is designed to ingest that.")


    parser.add_argument("--resource_file",   type=str, default=None, help="Path to resource file (e.g., JGF). Not alongside --resource_R.")
    parser.add_argument("--resource_R",      type=str, default=None, help="Path to Rlist file. Not alongside --resource_file.")

    parser.add_argument("--nnodes",          type=int, default=0, help="Number of nodes (not alongside --resource_file/--resource_R).")
    parser.add_argument("--nsockets",        type=int, default=1, help="CPU sockets per node. Default: 1")
    parser.add_argument("--ncpus",           type=int, default=1, help="CPU cores per socket. Default: 1")
    parser.add_argument("--ngpus",           type=int, default=0, help="GPUs per node. Default: 0")

    parser.add_argument("--log-level",       type=int, default=10, help="Python logger level.")
    parser.add_argument("--log-file",        type=str, default=None, help="Write logs to this file; default stdout.")

    parser.add_argument("--exclusive",       action="store_true", help="Each job consumes all resources on its nodes.")
    parser.add_argument("--quiet",           action="store_true", help="Turn off console logs")

    parser.add_argument("--backend",         type=str, default="flux", help="Determines what backend to use for job management. Currently, only Flux and Mock are supported as this is mainly for unit testing.")
    parser.add_argument("--batch_job_starts",action="store_false", help="Option to smooth out the confirmation of job start events in the exec module. Can subtley affect scheduling behavior.")
    parser.set_defaults(account_system_latency=None)
    parser.add_argument("--account-system-latency", "--account_system_latency", dest="account_system_latency", action="store_true", help="Stamp starts from current faketime after the start ack, folding scheduler/emulator latency into job timing.")
    parser.add_argument("--no-account-system-latency", "--no_account_system_latency", dest="account_system_latency", action="store_false", help="Stamp starts from simulation event time, ignoring scheduler/emulator latency.")
    parser.set_defaults(jobtap_logging=None)
    parser.add_argument("--jobtap_logging", "--jobtap-logging", dest="jobtap_logging", action="store_true", help="Enable verbose emu-jobtap logging.")
    parser.add_argument("--no_jobtap_logging", "--no-jobtap-logging", dest="jobtap_logging", action="store_false", help="Disable verbose emu-jobtap logging.")

    parser.add_argument("--output_dir",      type=str, default="./", help="Directory to dump all output files to. Does not include log file.")

    parser.add_argument("--faketime_timestamp_file", "--faketime-timestamp-file", dest="faketime_timestamp_file", type=str, default=None, help="Enable libfaketime integration using this FAKETIME_TIMESTAMP_FILE path.")
    parser.add_argument("--faketime_initial_epoch", "--faketime-initial-epoch", dest="faketime_initial_epoch", type=float, default=None, help="Fake Unix timestamp corresponding to simulation time zero.")
    parser.add_argument("--faketime_tolerance", "--faketime-tolerance", dest="faketime_tolerance", type=float, default=None, help="Slack in seconds before treating fake time as already at or past a target.")
    parser.add_argument("--faketime_near_event_threshold", "--faketime-near-event-threshold", dest="faketime_near_event_threshold", type=float, default=None, help="If the next event is this many seconds or less away in fake time, wait naturally instead of jumping.")
    parser.add_argument("--faketime_seed", "--faketime-seed", dest="faketime_seed", action="store_true", default=None, help="Seed the faketime timestamp file at startup.")
    parser.add_argument("--faketime_no_seed", "--faketime-no-seed", dest="faketime_seed", action="store_false", help="Do not seed the faketime timestamp file at startup; require an existing relative offset.")
    parser.set_defaults(otel_enabled=None)
    parser.add_argument("--otel", "--otel-enabled", dest="otel_enabled", action="store_true", help="Enable profiling through the out-of-process OpenTelemetry bridge.")
    parser.add_argument("--no-otel", "--otel-disabled", dest="otel_enabled", action="store_false", help="Disable profiling through the out-of-process OpenTelemetry bridge.")
    parser.add_argument("--otel-endpoint", dest="otel_endpoint", type=str, default=None, help="OTLP HTTP trace endpoint used by the bridge process.")
    parser.add_argument("--otel-service-name", dest="otel_service_name", type=str, default=None, help="Base OpenTelemetry service name for Flux Fiction profiling.")
    parser.add_argument("--otel-bridge-socket", dest="otel_bridge_socket", type=str, default=None, help="Unix datagram socket path used by profiled processes to talk to the bridge.")
    parser.add_argument("--otel-summary-file", dest="otel_summary_file", type=str, default=None, help="Optional profiling summary output path written by the bridge.")
    parser.add_argument("--otel-spans-file", dest="otel_spans_file", type=str, default=None, help="Optional JSONL span dump path written by the bridge.")
    parser.add_argument("--otel-bridge-log-file", dest="otel_bridge_log_file", type=str, default=None, help="Optional log file for the bridge process.")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    # Intentionally route CLI execution through the legacy args wrapper.
    # New code should prefer the API-level ExperimentConfig entrypoint.
    result = client.run_experiment_from_args(args)

    if not result.ok:
        logger.critical(f"Run failed: {result.message}")
        return 1

    logger.info(result.message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
