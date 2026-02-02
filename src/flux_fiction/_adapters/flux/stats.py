import subprocess
import json

def get_module_stats_anyhow(flux_handle, module_name: str) -> dict:
    """
    Best-effort way to get `flux module stats <name>` as a dict.
    """
    # try:
    #     return flux_handle.rpc("module.stats", payload={"name": module_name}).get()
    # except Exception:
    #     print("didnt work")
    
    out = subprocess.check_output(["flux", "module", "stats", module_name], text=True)
    return json.loads(out)

def get_content_sqlite_dbfile_size(flux_handle) -> int:
    st = get_module_stats_anyhow(flux_handle, "content-sqlite")
    # your example shows dbfile_size at the top-level
    return int(st.get("dbfile_size", 0))