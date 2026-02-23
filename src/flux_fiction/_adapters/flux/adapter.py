# flux_fiction/_adapters/flux/adapter.py
from __future__ import annotations
import flux
import logging
from collections.abc import Callable
from . import stats
from . import journal
from . import modules
from . import resources
from . import watchers

logger = logging.getLogger(__name__)

class FluxAdapter:
    def __init__(self) -> None:
        self._handle: flux.Flux | None = None
        self._watchers = None
        self._services = None
        self.simulation = None
        self.consumer = None
        self._pending_start_msgs: dict[int, object] = {}
        self._pending_complete_msgs: dict[int, object] = {}

    def open(self, simulation) -> None:
        if self._handle is None:
            self._handle = flux.Flux()
        if simulation is None: 
            raise ValueError("valid Simulation object required to start FluxAdapter")
        self.simulation = simulation

    def close(self) -> None:
        for job in self._pending_start_msgs:
            logger.warning(f"Job {job} was allocated but not started at the end of simulation")
            # self.cancel_job(job)
        for job in self._pending_complete_msgs:
            logger.warning(f"Job {job} was started but did not complete at the end of simulation")
            # self.cancel_job(job)
        if self._handle is None:
            return
        modules.reset_jobtap_plugin(self._handle)
        if self._watchers is not None:
            watchers.teardown_watchers(self._handle, self._watchers, self._services or set())
        self._watchers = None
        self._services = None

# TODO: add in a parameter to allow you to just specify module parameters instead of putting a 
# function paramater for every single module paramter 

#TODO Fix this to where it will try to load the jobtap plugin if it isn't loaded


# TODO: Make the journal into a logging feature 
        

#TODO See if I can use this with the exec module? Then again it might be good to have a standard interface from Flux to engine and then from engine to the exec system. 

    def install_resources(self, cfg):
        ''''''
        resources.insert_resource_data(self._handle, cfg.nnodes, cfg.ncpus)
        # insert_resource_R_from_json(handle, rjson_path="/home/j/Desktop/flux/sc25_poster/flux-fiction/src/python/tuolumne.json")
    
    def reload_scheduler(self, cfg):
        ''''''
        #TODO Make this configurable from the config file
        modules.reload_modules(self._handle, queue_policy="conservative", match_policy="first")
        modules.load_missing_modules(self._handle)
    
    def register_exec_service(self):
        ''''''
        watchers.exec_hello(self._handle)

    def register_job_tracking(self):
        self.consumer = journal.setup_journal(self._handle)
    
    def arm_watchers(self):
        ''''''
        self._watchers, self._services = watchers.setup_watchers(self._handle, self.simulation.start_job, self._pending_start_msgs, self._pending_complete_msgs)
    
    def start_reactor(self):
        ''''''
        try: 
            self._handle.reactor_run(self._handle.get_reactor(), 0)
        except Exception as e:
            raise RuntimeError("Failed to run Flux reactor") from e


    def stop_reactor(self):
        ''''''
        self._handle.reactor_stop(self._handle.get_reactor())
            
    def get_kvs_stats(self) -> dict:
        return stats.get_kvs_stats(self._handle)
    
    def query_quiescent(self, json_string, return_cb):
        logger.debug("Querying quiescent")
        try:
            self._handle.rpc(
                "job-manager.emu-jobtap.quiescent",
                payload=json_string).then(safe_then(return_cb), arg=None)
        except Exception as e:
            raise RuntimeError("Failed to query quiescent") from e
          
    
    def get_eventlog(self, jobid):
        return flux.job.job_kvs_lookup(self._handle, jobid, keys=["eventlog"])
    
    def get_formatted_id(self, job_id):
        return flux.job.JobID(job_id).f58

    def nodelist_lookup(self, jobid) -> list[int]:
        if self._handle is not None:
            nodes = stats.flux_nodelist_by_id(self._handle, jobid)
            return nodes, "flux_nodelist" if nodes else "missing"
        else:
            raise Exception("FluxAdapter.nodelist_lookup: flux handle is None")
        
    def submit_job(self, jobspec_json) -> int:
        return flux.job.submit(self._handle, jobspec_json)
                
    def cancel_job(self, jobid):
        return flux.job.RAW.cancel(self._handle, jobid, "Canceled by emulator")
    
    def ack_start(self,jobid):
        msg = self._pending_start_msgs.pop(jobid, None)
        self._pending_complete_msgs[jobid] = msg
        self._handle.respond(msg,payload={"id": jobid, "type": "start", "data": {}})

    def ack_complete(self, jobid):
        msg = self._pending_complete_msgs.pop(jobid, None)
        self._handle.respond(
            msg,
            payload={"id": jobid, "type": "finish", "data": {"status": 0}}
        )
        self._handle.respond(
            msg,
            payload={"id": jobid, "type": "release",
                     "data": {"ranks": "all", "final": True}}
        )
            

def safe_then(cb):
    def _wrapped(fut, arg):
        try:
            return cb(fut, arg)
        except Exception:
            logger.exception("Exception inside .then callback %s", cb.__name__)
            raise
    return _wrapped

