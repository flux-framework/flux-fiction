from __future__ import annotations

import argparse

from flux_fiction.api import client

import logging
logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument("--job_traces",      type=str, default=None, help="Path to historical job data/traces. Can be defined in config file.")
    parser.add_argument("--config_file",     type=str, default=None, help="Path to configuration TOML (optional but recommended).")

    parser.add_argument("--resource_file",   type=str, default=None, help="Path to resource file (e.g., JGF). Not alongside --resource_R.")
    parser.add_argument("--resource_R",      type=str, default=None, help="Path to Rlist file. Not alongside --resource_file.")

    parser.add_argument("--nnodes",          type=int, default=0, help="Number of nodes (not alongside --resource_file/--resource_R).")
    parser.add_argument("--nsockets",        type=int, default=1, help="CPU sockets per node. Default: 1")
    parser.add_argument("--ncpus",           type=int, default=1, help="CPU cores per socket. Default: 1")
    parser.add_argument("--ngpus",           type=int, default=0, help="GPUs per node. Default: 0")

    parser.add_argument("--log-level",       type=int, default=10, help="Python logger level.")
    parser.add_argument("--log-file",        type=str, default=None, help="Write logs to this file; default stdout.")

    parser.add_argument("--exclusive", action="store_true", help="Each job consumes all resources on its nodes.")
    parser.add_argument("--quiet",     action="store_true", help="Turn off console logs")


    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    result = client.run_experiment(args)

    if not result.ok:
        logger.critical(f"Run failed: {result.message}")
        return 1

    logger.info(result.message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
