# flux_fiction/_adapters/flux/adapter.py
from __future__ import annotations
import flux
import flux
import logging
import json
from . import stats
from . import journal
from . import modules
from . import resources
from . import watchers

logger = logging.getLogger(__name__)

class FluxAdapter:
    def __init__(self) -> None:
        self._handle = None
        self.watchers = None
        self.services = None
        self.consumer = None 

        self._handle = flux.Flux()
        if self._handle is None:
            raise RuntimeError("Unable to get Flux handle. Is Flux currently running?")
    
    def configure_backend(self, configuration: object, start_job_cb: function) -> None:
        '''Configure the backend to use given resource configuration'''
        resources.insert_resource_data(self._handle, configuration.nnodes, configuration.ncpus)
        # insert_resource_R_from_json(handle, rjson_path="/home/j/Desktop/flux/sc25_poster/flux-fiction/src/python/tuolumne.json")
        
        # TODO: add in a parameter to allow you to just specify module parameters instead of putting a 
        # function paramater for every single module paramter 
        modules.reload_modules(self._handle, queue_policy="conservative", match_policy="first")

        #TODO Fix this to where it will try to load the jobtap plugin if it isn't loaded
        modules.load_missing_modules(self._handle)

        self.watchers, self.services = watchers.setup_watchers(self._handle, start_job_cb)

        # TODO: Remove everything related to journal consumer. This is junk 
        self.consumer = journal.setup_journal(self._handle)

        # Register the exec system simulator as the exec system that we are using for Flux
        #TODO See if I can use this with the exec module? Then again it might be good to have a standard interface from Flux to engine and then from engine to the exec system. 
        watchers.exec_hello(self._handle)

    def close(self) -> None:
        '''Shutdown any persistent services on the backend (e.g., watchers, reactor)'''
        watchers.teardown_watchers(self._handle, self.watchers, self.services)

        # Reset the emu-jobtap probe to defaults after a run
        try:
            self._handle.rpc("job-manager.emu-jobtap.reset",
                            payload={"keep_timestep": False}).get()
            logger.debug("Reset emu-jobtap probe to defaults")
        except Exception as e:
            logger.error(f"Failed to reset emu-jobtap probe: {e}")
            #TODO Forward this exception to the engine somehow
            # return EngineResult(ok=False, message="Failed to reset emu-jobtap probe")
    
    def end_simulation(self) -> None:
        self._handle.reactor_stop(self._handle.get_reactor())

    def reactor_run(self):
        try: 
            self._handle.reactor_run(self._handle.get_reactor(), 0)
        except: 
            raise RuntimeError("Failed to run Flux Reactor")

    def get_handle(self) -> flux.Flux:
        if self._handle is None:
            logger.warning("Handle was None at invocation of get_handle(). This is not normal behavior.")
            self._handle = flux.Flux()
            if self._handle is None:
                raise RuntimeError("Unable to get Flux handle. Is Flux currently running?")
        return self._handle
            
    def get_kvs_stats(self) -> dict:
        return stats.get_kvs_stats(self._handle)
    
    def query_quiescent(self, json_string, return_cb):
        logger.debug("Querying quiescent")
        try:
            self._handle.rpc(
                "job-manager.emu-jobtap.quiescent",
                payload=json_string).then(safe_then(return_cb), arg=None)
        except Exception as e:
            logger.debug(e)
            return
          
    
    def get_eventlog(self, jobid):
        return flux.job.job_kvs_lookup(self._handle, jobid, keys=["eventlog"])
    
    def get_formatted_id(self, job_id):
        return flux.job.JobID(job_id).f58

    def nodelist_lookup(self, jobid):
        if self._handle is not None:
            nodes = stats.flux_nodelist_by_id(self._handle, jobid)
            return nodes, "flux_nodelist" if nodes else "missing"
        else:
            raise Exception("FluxAdapter.nodelist_lookup: flux handle is None")
        
    def submit_job(self, jobspec_json) -> int:
        return flux.job.submit(self._handle, jobspec_json)
                
    def cancel_job(self, jobid):
        return flux.job.RAW.cancel(self._handle, jobid, "Canceled by emulator")
    
    def start_job(self, msg, jobid):
        self._handle.respond(msg,payload={"id": jobid, "type": "start", "data": {}})

    def complete_job(self, msg, jobid):
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
            # This will raise if the RPC returned an error
            fut.get()
        except Exception:
            logger.exception("RPC future failed before callback %s", cb.__name__)
            raise
        try:
            return cb(fut, arg)
        except Exception:
            logger.exception("Exception inside .then callback %s", cb.__name__)
            raise
    return _wrapped
