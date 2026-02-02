from __future__ import annotations

from flux_fiction._core import engine
import flux_fiction.api.config as config

import logging
logger = logging.getLogger(__name__)

#TODO Make it where this will make in an args dict or possibly a ExperimentConfig object
def run_experiment(args: dict) -> engine.EngineResult:
    '''
    Docstring for run_experiment
    
    :param args: parsed command line arguments for a run of Flux Fiction
    :type args: dict
    :return: Return code for a run of the experiment
    :rtype: EngineResult

    This function will use the core Flux Fiction library to execute a single experiment run.
    '''
    cfg = config.from_cli_args(args)
    config.setup_logging(level=cfg.log_level, log_file=cfg.log_file, quiet=cfg.quiet)

    logger.info(f"Running experiment with config: {cfg}")
    return engine.run(cfg)
