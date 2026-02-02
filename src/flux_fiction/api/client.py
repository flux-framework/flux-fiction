from __future__ import annotations

from flux_fiction._core import engine
import flux_fiction.api.config as config


class Run:
    

def run_experiment(args: dict) -> engine.EngineResult:
    cfg = config.from_cli_args(args)

    print(f"[api] Running experiment with config: {config}")
    return engine.run(config)
