from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Any
import logging
import sys
import os
from pydantic import BaseModel, Field, model_validator, ValidationError, ConfigDict


logger = logging.getLogger(__name__)

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
    quiet: bool = False

    backend: Optional[str] = "flux"

class ExperimentConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_traces: Optional[str] = None
    config_file: Optional[str] = None

    resource_file: Optional[str] = None
    resource_R: Optional[str] = None

    nnodes: int = Field(default=0, ge=0)
    nsockets: int = Field(default=1, ge=1)
    ncpus: int = Field(default=1, ge=1)
    ngpus: int = Field(default=0, ge=0)

    log_level: int = Field(default=10, ge=0)
    log_file: Optional[str] = None

    exclusive: bool = False
    quiet: bool = False

    backend: Optional[str] = "flux"

    @model_validator(mode="after")
    def _checks(self):
        if self.resource_file and self.resource_R:
            raise ValueError("Use only one of resource_file or resource_R.")
        if self.job_traces is None and self.config_file is None:
            raise ValueError("No job_traces input. Provide job_traces (or set it in TOML).")
        if self.backend != "flux" and self.backend != "mock":
            raise ValueError("Backend must be set to either flux or mock.")
        return self

def _to_dataclass(data: dict) -> ExperimentConfig:
    try:
        m = ExperimentConfigModel.model_validate(data)
    except ValidationError as e:
        # make CLI/TOML errors readable
        raise ValueError(str(e)) from e

    return ExperimentConfig(**m.model_dump())

def _load_toml(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"TOML config file not found: {path}")

    # Python 3.11+: tomllib. Older: tomli.
    try:
        import tomllib  # type: ignore
        with open(path, "rb") as f:
            return tomllib.load(f)
    except ModuleNotFoundError:
        import tomli  # type: ignore
        with open(path, "rb") as f:
            return tomli.load(f)
        
# TODO write a toml parser for configuration
def from_toml(args: dict) -> ExperimentConfig:
    '''
    from_toml
    ----------
    
    :param args: Description
    :type args: dict
    :return: Description
    :rtype: ExperimentConfig

    Build ExperimentConfig from a TOML file using Pydantic validation.

    Supports either:
      - Top-level keys, or
      - A [flux_fiction] table.
    '''
    cfg_path = args.config_file
    if not cfg_path:
        raise ValueError("from_toml called but args.config_file is missing")
    
    raw = _load_toml(cfg_path)
    
    data = raw.get("flux_fiction", raw)
    if not isinstance(data, dict):
        raise ValueError("TOML config must be a table (dict-like)")
    
        # Normalize + inject config_file path
    merged: dict[str, Any] = dict(data)
    merged["config_file"] = cfg_path

    # Optional override: CLI job_traces wins if provided
    cli_job_traces = getattr(args, "job_traces", None) if not isinstance(args, dict) else args.get("job_traces")
    if cli_job_traces is not None:
        merged["job_traces"] = cli_job_traces

    try:
        model = ExperimentConfigModel.model_validate(merged)
    except ValidationError as e:
        raise ValueError(f"Invalid TOML config in {cfg_path}:\n{e}") from e

    return ExperimentConfig(**model.model_dump())
    

def from_cli_args(args) -> ExperimentConfig:
    '''
    from_cli_args
    --------------
    
    :param args: parsed command line arguments given to Flux Fiction
    :return: Returns an ExperimentConfig object containing the full configuration being used for the Flux Fiction run
    :rtype: ExperimentConfig

    This function is used as a loader to take a set of command line arguments and turn it into a configuration for a run of Flux Fiction
    '''
    data = vars(args).copy()  

    if data.get("config_file"):
        return from_toml(args)

    return _to_dataclass(data)

def setup_logging(*, level: int, log_file: Optional[str] = None, quiet: bool = False) -> None:
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
    if quiet == False:
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        root.addHandler(sh)

    # Optional file handler
    if log_file:
        fh = logging.FileHandler(log_file, mode="w")
        fh.setFormatter(fmt)
        root.addHandler(fh)