import logging 
logger = logging.getLogger(__name__)

def get_loaded_modules(flux_handle):
    """
    Retrieve the list of loaded modules in the current Flux instance.
    """
    try:
        modules = flux_handle.rpc("module.list").get()["mods"]
        return modules
    except Exception as e:
        raise RuntimeError(f"Error retrieving loaded modules: {e}")


def load_missing_modules(flux_handle):
    # TODO: check that necessary modules are loaded
    # if not, load them
    # return an updated list of loaded modules
    # Should be checking for the jobtap module
    loaded_modules = get_loaded_modules(flux_handle)
    pass

def reset_jobtap_plugin(flux_handle, *, keep_timestep=False, batch_job_starts=True):
    try:
        flux_handle.rpc(
            "job-manager.emu-jobtap.reset",
            payload={
                "keep_timestep": keep_timestep,
                "batch_job_starts": batch_job_starts,
            },
        ).get()
        logger.debug(
            "Reset emu-jobtap probe to defaults (batch_job_starts=%s)",
            batch_job_starts,
        )
    except Exception as e:
        logger.error(f"Failed to reset emu-jobtap probe: {e}")

import copy
import json
import logging
from pathlib import Path




def _load_config_object(config_source):
    """
    Accept either:
      - a Python dict already produced from `flux config get`
      - a path to a .json file containing that object

    Returns a deep-copied dict so we can mutate it safely.
    """
    if config_source is None:
        return {}

    if isinstance(config_source, dict):
        return copy.deepcopy(config_source)

    if isinstance(config_source, (str, Path)):
        path = Path(config_source)
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    raise TypeError(
        f"config_source must be dict, str, Path, or None; got {type(config_source)!r}"
    )


def _extract_qmanager_only_config(config_obj):
    """
    Return a config object containing only the sched-fluxion-qmanager table.

    This avoids pushing any [resource] config that might interfere with
    resource module args like noverify / monitor-force-up.
    """
    cfg = {}
    qmgr = config_obj.get("sched-fluxion-qmanager")
    if isinstance(qmgr, dict):
        cfg["sched-fluxion-qmanager"] = copy.deepcopy(qmgr)
    return cfg


def reload_modules(flux_handle, config_source=None):
    """
    Reload resource + scheduler modules in the order:

      Sched Unload -> Res Unload -> config.load(qmanager only)
      -> Res Load(with raw args) -> Sched Load

    config_source may be:
      - dict from `flux config get`
      - path to a JSON file containing that object
      - None (skip config.load entirely)
    """
    sched_module = "sched-simple"
    sched_simple_path = None
    resource_module_path = None
    fluxion_qmanager_path = None
    fluxion_resource_path = None
    feasibility_module_path = None

    config_source = "/home/j/Desktop/flux/sc25_poster/flux-fiction/experiment_data/ff_traces/experiment_scheduler_easy_resdepth32_20260330_165549/flux_config.json"

    for module in get_loaded_modules(flux_handle):
        logger.debug("loaded module: %s", module)

        services = module.get("services", [])
        name = module.get("name", "")

        if "sched-simple" in services:
            sched_module = module["name"]
            sched_simple_path = module["path"]
        elif "sched-fluxion-qmanager" in name:
            sched_module = "fluxion"
            fluxion_qmanager_path = module["path"]
        elif "sched-fluxion-resource" in name:
            fluxion_resource_path = module["path"]
        elif name == "resource" or "resource" in name:
            resource_module_path = module["path"]
        elif "feasibility" in name:
            feasibility_module_path = module["path"]

    logger.debug("Reloading '%s' and 'resource' modules", sched_module)

    if resource_module_path is None:
        raise RuntimeError(
            "Unable to get resource module path (is the resource module loaded?)"
        )

    # 1. Unload scheduler + resource
    try:
        if sched_module == "sched-simple":
            flux_handle.rpc(
                "module.remove",
                payload={"name": "sched-simple"},
            ).get()
        else:
            flux_handle.rpc(
                "module.remove",
                payload={"name": "sched-fluxion-qmanager"},
            ).get()
            flux_handle.rpc(
                "module.remove",
                payload={"name": "sched-fluxion-feasibility"},
            ).get()
            flux_handle.rpc(
                "module.remove",
                payload={"name": "sched-fluxion-resource"},
            ).get()

        flux_handle.rpc(
            "module.remove",
            payload={"name": "resource"},
        ).get()

    except Exception as e:
        logger.error("Error removing modules: %s", e)
        raise

    # 2. Load ONLY qmanager config, if provided
    if config_source is not None:
        try:
            full_cfg = _load_config_object(config_source)
            qmgr_cfg = _extract_qmanager_only_config(full_cfg)

            if qmgr_cfg:
                logger.debug(
                    "Loading qmanager-only config via config.load:\n%s",
                    json.dumps(qmgr_cfg, indent=2, sort_keys=True),
                )
                flux_handle.rpc("config.load", payload=qmgr_cfg).get()
            else:
                logger.warning(
                    "No sched-fluxion-qmanager table found in config source; "
                    "skipping config.load"
                )

        except Exception as e:
            logger.error("Error loading qmanager-only config: %s", e)
            raise

    # 3. Reload resource + scheduler
    try: 
        flux_handle.rpc("module.load", payload={"path": resource_module_path,
                                                "args": [ "noverify", "monitor-force-up"]}).get()

        flux_handle.rpc("module.load", payload={"path": fluxion_resource_path,
                                                "args": [f"match-policy=lonodex"]}).get()

        flux_handle.rpc("module.load", payload={"path": feasibility_module_path,
                                                "args": []}).get()

        flux_handle.rpc("module.load", payload={"path": fluxion_qmanager_path,
                                                "args": []}).get()

    except Exception as e:
        logger.error("Error loading modules: %s", e)
        raise