# flux_fiction/_adapters/base.py
from __future__ import annotations
from typing import Protocol, Any, Callable

class Adapter(Protocol):
    # def configure_backend(self, configuration: object, start_job_cb: function) -> None:
    #     '''Configure the backend to use given resource configuration. Requires reference to the Simulation\'s start job callback to properly handle interaction with the job manager.'''
    def open(self, simulation: object) -> None:
        '''Connect to the resource manager'''

    def close(self) -> None:
        '''Shutdown any persistent services on the backend (e.g., watchers, reactor) and disconnect from the resource manager'''
    
    def install_resources(self, cfg: object) -> None:
        '''Register emulated resources with the resource manager'''

    def reload_scheduler(self, cfg: object) -> None:
        '''Restart the scheduler and associated modules'''

    def register_exec_service(self) -> None:
        '''Register the applicable emulated exec service'''

    def register_job_tracking(self) -> None:
        '''Setup logging for job state changes'''

    def arm_watchers(self) -> None:
        '''Setup and configure any watchers'''
    
    def start_reactor(self) -> None:
        '''Start the system reactor'''
    
    def stop_reactor(self) -> None:
        '''Stop the system reactor'''

    def get_kvs_stats(self) -> dict:
        '''Return the size of KVS (Flux specific)'''

    def query_quiescent(self, json_string: str, return_cb: Callable) -> None:
        '''Query whether Flux is quiescent'''
    
    def get_eventlog(self, jobid: int) -> dict[str, Any]:
        '''Get the eventlog for a job'''

    def get_formatted_id(self, job_id: int) -> str:
        '''Get the jobid in f58 format'''
    
    def nodelist_lookup(self, jobid: int) -> list[int]:
        '''Get the nodelist for a job'''

    def submit_job(self, jobspec_json: str) -> int:
        '''Submit a new job to the resource manager'''

    def cancel_job(self, jobid: int) -> None:
        '''Cancel the job with {jobid} job id'''
    
    def ack_complete(self, jobid: int) -> None:
        '''Send '''
    
    def ack_start(self,jobid: int) -> None:
        ''''''
