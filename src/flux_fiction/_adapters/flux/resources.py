import os
import json
import flux
from flux.resource import Rlist

import logging 
logger = logging.getLogger(__name__)

def attach_scheduling_graph(rlist_json: dict, scheduling: dict) -> dict:
    """
    Mirror cmd_parse_config(): rl->scheduling = sched_json
    No conversion, no wrapping (unless you choose to).
    """
    if not isinstance(rlist_json, dict):
        raise TypeError("rlist_json must be a dict")
    if not isinstance(scheduling, dict):
        raise TypeError("scheduling must be a dict (parsed JSON object)")
    rlist_json["scheduling"] = scheduling
    return rlist_json


def load_json_file(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)

def insert_resource_data(flux_handle, num_ranks, cores_per_rank,
                         hostname_pattern="node{rank}", gpus_per_rank=0,
                         scheduling_path=None, scheduling_obj=None):
    """
    Build R using Rlist.add_rank(...) for cores, then add GPU children per rank
    with rlist.add_child(rank, "gpu", "<idset>"). Debug-print the final JSON
    before committing to KVS.
    """
    if num_ranks <= 0 or cores_per_rank <= 0:
        raise ValueError("Number of ranks and cores per rank must be positive integers")

    rlist = Rlist()

    for rank in range(num_ranks):
        core_range = f"0-{cores_per_rank - 1}" if cores_per_rank > 1 else "0"
        hostname = hostname_pattern.format(rank=rank)

        # Add the node with its core children
        rlist.add_rank(rank, hostname=hostname, cores=core_range)

        # If GPUs enabled, append a gpu child ID set to this rank
        if gpus_per_rank and gpus_per_rank > 0:
            gpu_range = f"0-{gpus_per_rank - 1}" if gpus_per_rank > 1 else "0"
            rlist.add_child(rank, "gpu", gpu_range)

    # Encode to the R JSON string
    rlist_str = rlist.encode()
    try:
        rlist_json = json.loads(rlist_str)
    except Exception:
        rlist_json = {"RAW_R": rlist_str}

    logger.debug("resource.R going to KVS:\n%s",
                 json.dumps(rlist_json, indent=2, sort_keys=True))

    # Attach scheduling JSON
    if scheduling_path and scheduling_obj:
        raise ValueError("Use only one of scheduling_path or scheduling_obj")

    if scheduling_path:
        sched = load_json_file(scheduling_path)
        out = attach_scheduling_graph(rlist_json, sched)
        if out is not None:
            rlist_json = out
    elif scheduling_obj:
        out = attach_scheduling_graph(rlist_json, scheduling_obj)
        if out is not None:
            rlist_json = out

    # NOW debug-print the final thing
    print("FINAL resource.R going to KVS:\n%s",
                 json.dumps(rlist_json, indent=2, sort_keys=True))


    # Put into KVS and commit
    kvs_key = "resource.R"
    put_rc = flux.kvs.put(flux_handle, kvs_key, rlist_json)
    if put_rc is not None:
        raise ValueError(f"Error inserting resource data into KVS, rc={put_rc}")

    commit_rc = flux.kvs.commit(flux_handle)
    if commit_rc is not None:
        raise ValueError(f"Error committing resource data to KVS, rc={commit_rc}")
    
    print("Rlist ENCODE():", rlist.encode())

def insert_resource_R_from_json(flux_handle, rjson_path: str):
    """
    Load a complete RFC20 / RV1 resource description from JSON
    and install it into KVS as resource.R.

    This mirrors the C path:
      json_load_file -> rlist_from_json -> rlist_puts
    except we already trust the JSON is valid.
    """
    if not os.path.exists(rjson_path):
        raise FileNotFoundError(f"Resource JSON file not found: {rjson_path}")

    with open(rjson_path, "r") as f:
        rjson = json.load(f)

    if not isinstance(rjson, dict):
        raise ValueError("resource.R JSON must be a JSON object")

    logger.info("Loading full resource.R from %s", rjson_path)

    # Put directly into KVS
    rc = flux.kvs.put(flux_handle, "resource.R", rjson)
    if rc is not None:
        raise RuntimeError(f"flux.kvs.put(resource.R) failed, rc={rc}")

    rc = flux.kvs.commit(flux_handle)
    if rc is not None:
        raise RuntimeError(f"flux.kvs.commit() failed, rc={rc}")
