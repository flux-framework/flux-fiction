from typing import Sequence, Union, List
import re
import json 
import csv

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

def nodelist_lookup(jobid, job, flux_handle):
    if flux_handle is not None:
        nodes = flux_nodelist_by_id(flux_handle, jobid)
        return nodes, "flux_nodelist" if nodes else "missing"
    else:
        raise Exception("dump_transitions_to_csv: You didn't pass the flux handle")
        
def dump_transitions_to_csv(
    simulation,
    filename="job_transitions.csv",
    flux_handle=None,
):
    """
    Writes job transitions CSV and adds NODELIST (comma-separated ints).
    Nodelist is resolved via Flux job-list.list-id.
    """
    def f(x):
        return "" if x in (None, "") else f"{float(x):.6f}"

    rows = []
    for jobid, job in simulation.job_map.items():
        nodes, _ = nodelist_lookup(jobid, job, flux_handle)
        rows.append({
            "trace_idx": job.trace_index,
            "jobid": jobid,
            "nnodes": job.nnodes,
            "SUBMIT": f(job.state_transitions.get("SUBMITTED", "")),
            "START":  f(job.state_transitions.get("STARTED", "")),
            "FINISH": f(job.state_transitions.get("COMPLETED", "")),
            "REAL_SUBMIT": f(job.real_submit),
            "REAL_START":  f(job.real_start),
            "REAL_FINISH": f(job.real_finish),
            "NODELIST": ",".join(str(n) for n in nodes),
        })

    rows.sort(key=lambda r: (
        r["trace_idx"] if r["trace_idx"] is not None else 10**9,
        float(r["REAL_SUBMIT"]) if r["REAL_SUBMIT"] else float("inf"))
    )

    fieldnames = ["trace_idx", "jobid", "nnodes",
                  "SUBMIT", "START", "FINISH",
                  "REAL_SUBMIT", "REAL_START", "REAL_FINISH",
                  "NODELIST"]
    with open(filename, "w", newline="") as csvfile:
        w = csv.DictWriter(csvfile, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def _sec_to_us(x: Union[str, float, int]) -> int:
    if x in ("", None):
        return 0
    return int(float(x) * 1_000_000.0)

def write_per_node_chrome_trace(simulation, out_path="pernode_trace.json", flux_handle=None, pid: int = 1):
    """
    Generates a Chrome/Perfetto trace with one lane per node:
      - tid = node index
      - name = job{trace_idx}:{jobid}
      - ts/dur from START..FINISH (sim time only; no submit/real times)

    Open the JSON in https://ui.perfetto.dev to see an occupancy chart.
    """
    events = []
    threads_emitted = set()

    # Process label
    events.append({
        "name": "process_name", "ph": "M", "pid": pid,
        "args": {"name": "Cluster"}
    })
    
    # Build slices
    for jobid, job in simulation.job_map.items():
        start_s = job.state_transitions.get("STARTED", None)
        finish_s = job.state_transitions.get("COMPLETED", None)
        if start_s in (None, "") or finish_s in (None, ""):
            continue
        ts_us = _sec_to_us(start_s)
        dur_us = _sec_to_us(finish_s) - ts_us
        if dur_us <= 0:
            continue

        nodes, src = nodelist_lookup(jobid, job, flux_handle)
        if not nodes:
            continue

        for n in nodes:
            if n not in threads_emitted:
                events.append({
                    "name": "thread_name", "ph": "M", "pid": pid, "tid": int(n),
                    "args": {"name": f"node {n}"}
                })
                events.append({
                    "name": "thread_sort_index", "ph": "M", "pid": pid, "tid": int(n),
                    "args": {"sort_index": int(n)}
                })
                threads_emitted.add(n)

            events.append({
                "name": f"job{getattr(job, 'trace_index', '')}:{jobid}",
                "cat": "occupancy",
                "ph": "X",
                "pid": pid,
                "tid": int(n),
                "ts": ts_us,
                "dur": dur_us,
                "args": {"jobid": str(jobid), "trace_idx": getattr(job, "trace_index", None),
                         "nnodes": getattr(job, "nnodes", None), "_source": src}
            })

    trace_obj = {"traceEvents": events, "displayTimeUnit": "ms"}
    with open(out_path, "w") as f:
        json.dump(trace_obj, f)