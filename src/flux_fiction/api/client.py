from __future__ import annotations

from flux_fiction._core import engine
from flux_fiction._adapters.base import Adapter
from flux_fiction.api.config import ExperimentConfig, from_cli_args, setup_logging
from flux_fiction.api.status import RunStatusWriter

import logging
logger = logging.getLogger(__name__)


def make_adapter(cfg: ExperimentConfig) -> Adapter:
    if cfg.backend == "flux":
        from flux_fiction._adapters.flux.adapter import FluxAdapter
        return FluxAdapter()
    if cfg.backend == "mock":
        from flux_fiction._adapters.mock.adapter import MockAdapter
        return MockAdapter()
    raise ValueError(f"Unknown backend {cfg.backend!r}")


def run_experiment(
    cfg: ExperimentConfig,
    *,
    adapter: Adapter | None = None,
    status: RunStatusWriter | None = None,
) -> engine.EngineResult:
    '''
    run_experiment
    ---------------

    :param cfg: validated configuration for a run of Flux Fiction
    :type cfg: ExperimentConfig
    :return: Return code for a run of the experiment
    :rtype: EngineResult

    This function will use the core Flux Fiction library to execute a single experiment run.
    '''
    setup_logging(level=cfg.log_level, log_file=cfg.log_file, quiet=cfg.quiet)
    adapter = adapter or make_adapter(cfg)
    status = status or RunStatusWriter(cfg.status_file)

    logger.info(f"Running experiment with config: {cfg}")
    return engine.run(cfg, adapter, status=status)


def run_experiment_from_args(args) -> engine.EngineResult:
    '''
    Backward-compatible helper for legacy CLI call sites.
    New programmatic integrations should build an ExperimentConfig and call
    run_experiment() directly.
    '''
    cfg = from_cli_args(args)
    return run_experiment(cfg)
