from __future__ import annotations

import argparse
import logging

from flux_fiction.api import client
from flux_fiction.api.config import from_toml
from flux_fiction.api.status import RunStatusWriter

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one Flux Fiction experiment through the programmatic API."
    )
    parser.add_argument("--config-file", required=True, help="Generated Flux Fiction TOML config file.")
    parser.add_argument(
        "--faketime_timestamp_file",
        "--faketime-timestamp-file",
        dest="faketime_timestamp_file",
        default=None,
        help="libfaketime timestamp file path.",
    )
    parser.add_argument(
        "--faketime_seed",
        "--faketime-seed",
        dest="faketime_seed",
        action="store_true",
        default=None,
        help="Seed the libfaketime timestamp file before the run.",
    )
    parser.add_argument(
        "--faketime_no_seed",
        "--faketime-no-seed",
        dest="faketime_seed",
        action="store_false",
        help="Do not seed the libfaketime timestamp file before the run.",
    )
    parser.add_argument(
        "--faketime_tolerance",
        "--faketime-tolerance",
        dest="faketime_tolerance",
        type=float,
        default=None,
        help="Faketime tolerance override.",
    )
    parser.add_argument(
        "--faketime_near_event_threshold",
        "--faketime-near-event-threshold",
        dest="faketime_near_event_threshold",
        type=float,
        default=None,
        help="Faketime near-event threshold override.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = from_toml(args)
    status = RunStatusWriter(cfg.status_file)
    status.update(
        state="running",
        config_file=cfg.config_file,
        source_config_file=cfg.source_config_file,
        trace_file=cfg.job_traces,
    )

    result = client.run_experiment(cfg, status=status)
    if not result.ok:
        logger.critical("Run failed: %s", result.message)
        return 1

    logger.info(result.message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
