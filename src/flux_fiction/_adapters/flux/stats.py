import subprocess
import re
import json
from typing import Sequence, List, Union
import logging

logger = logging.getLogger(__name__)

#TODO See if I can not use subprocess here. Remove RPC comments
def get_module_stats(flux_handle, module_name: str) -> dict:
    """
    Get `flux module stats <name>` as a dict.
    """
    # try:
    #     return flux_handle.rpc("module.stats", payload={"name": module_name}).get()
    # except Exception:
    #     print("didnt work")
    
    out = subprocess.check_output(["flux", "module", "stats", module_name], text=True)
    return json.loads(out)

def get_kvs_stats(flux_handle) -> dict:
    st = get_module_stats(flux_handle, "content-sqlite")
    st.get("dbfile_size", 0)
    return st

def _expand_nodelist(nl: Union[str, Sequence[Union[str,int]]]) -> List[int]:
    """
    Convert typical Flux/host-style nodelists into integer node indices for lanes.
    Accepts:
      - "0,1,2-5,9"
      - "node[01-03,07]"  -> 1,2,3,7
      - ["node01","node02"] -> 1,2
      - [0,1,2]
    """
    if nl is None:
        return []
    if isinstance(nl, (list, tuple)):
        out = []
        for item in nl:
            if isinstance(item, int):
                out.append(item)
            else:
                m = re.search(r'(\d+)$', str(item))
                if m:
                    out.append(int(m.group(1)))
        return sorted(set(out))

    s = str(nl).strip()
    if not s:
        return []

    # bracketed ranges: prefix[1-3,7]
    m = re.match(r'^(.*)\[(.+)\]$', s)
    if m:
        inside = m.group(2)
        out = []
        for part in inside.split(','):
            part = part.strip()
            if '-' in part:
                a, b = part.split('-', 1)
                a, b = int(a), int(b)
                step = 1 if b >= a else -1
                for v in range(a, b + step, step):
                    out.append(v)
            else:
                out.append(int(part))
        return sorted(set(out))

    # plain list/ranges: "0,1,2-5,node12"
    out = []
    for part in s.split(','):
        part = part.strip()
        if not part:
            continue
        if '-' in part:
            a, b = part.split('-', 1)
            a, b = int(a), int(b)
            step = 1 if b >= a else -1
            out.extend(range(a, b + step, step))
        else:
            m = re.search(r'(\d+)$', part)
            out.append(int(m.group(1)) if m else int(part))
    return sorted(set(out))

def flux_nodelist_by_id(flux_handle, jobid):
    try:
        from flux.job.list import job_list_id, get_job
        rpc = job_list_id(flux_handle, int(jobid), attrs=["all"])
        info = rpc.get_jobinfo()  
        nl = getattr(info, "nodelist", "")
        nodes = _expand_nodelist(nl)
        if nodes:
            return nodes

        # Try unfiltered dict for inactive jobs 
        jd = get_job(flux_handle, int(jobid))
        if jd:
            nl = jd.get("nodelist", "")
            return _expand_nodelist(nl)
    except Exception:
        pass
    return []

