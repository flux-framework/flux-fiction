from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Any, Dict

@dataclass(frozen=True)
class ExperimentConfig:
    job_traces: Optional[str] = None
    config_file: Optional[str] = None

    resource_file: Optional[str] = None
    resource_R: Optional[str] = None

    nnodes: int = 0
    nsockets: int = 1
    ncpus: int = 1
    ngpus: int = 0

    log_level: int = 10
    log_file: Optional[str] = None

    exclusive: bool = False


def validate_config(cfg: ExperimentConfig) -> None:
    if cfg.resource_file and cfg.resource_R:
        raise ValueError("Use only one of --resource_file or --resource_R.")
    if cfg.nnodes < 0 or cfg.nsockets < 1 or cfg.ncpus < 1 or cfg.ngpus < 0:
        raise ValueError("Invalid resource counts.")
    if cfg.job_traces is None and cfg.config_file is None:
        raise ValueError("No job_traces input. Provide --job_traces")

# TODO write a toml parser for configuration
def from_toml(args: dict) -> ExperimentConfig:
    pass

def from_cli_args(args) -> ExperimentConfig:
    if args.config_file is None:
        cfg = ExperimentConfig(
            job_traces=args.job_traces,
            config_file=args.config_file,
            resource_file=args.resource_file,
            resource_R=args.resource_R,
            nnodes=args.nnodes,
            nsockets=args.nsockets,
            ncpus=args.ncpus,
            ngpus=args.ngpus,
            log_level=args.log_level,
            log_file=args.log_file,
            exclusive=args.exclusive,
        )
    else:
        cfg = from_toml(args)
        
    validate_config(cfg)
    return cfg
