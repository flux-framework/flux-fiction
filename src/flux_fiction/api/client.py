from __future__ import annotations

from flux_fiction._core import engine
import flux_fiction.api.config as config

import logging
import sys
from typing import Optional

def _setup_logging(*, level: int, log_file: Optional[str] = None) -> None:
    """
    Configure logging once for the whole application.
    All modules should only do `logger = logging.getLogger(__name__)`.
    """
    root = logging.getLogger()  # root logger
    root.setLevel(level)

    # Remove existing handlers to prevent duplicated logs in reruns/tests
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler (stderr)
    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    # Optional file handler
    if log_file:
        fh = logging.FileHandler(log_file, mode="w")
        fh.setFormatter(fmt)
        root.addHandler(fh)

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
    _setup_logging(level=cfg.log_level, log_file=cfg.log_file)

    print(f"[api] Running experiment with config: {cfg}")
    return engine.run(cfg)
