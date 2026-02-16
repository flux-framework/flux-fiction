# flux_fiction/_adapters/base.py
from __future__ import annotations
from typing import Protocol, Any

class Adapter(Protocol):
    def configure_backend(self, configuration: object, start_job_cb: function) -> None:
        '''Configure the backend to use given resource configuration. Requires reference to the Simulation\'s start job callback to properly handle interaction with the job manager.'''

    def close(self) -> None:
        '''Shutdown any persistent services on the backend (e.g., watchers, reactor)'''
    
    def end_simulation(self) -> None:
        '''Shuts down the simulation'''

    def reactor_run(self):
        '''run the reactor'''

    def get_handle(self) -> object:
        '''Return a backend handle (e.g., Flux Handle)'''

    def get_kvs_stats(self) -> dict:
        '''Return the size of KVS (Flux specific)'''

    def query_quiescent(self, json_string, return_cb):
        '''Query whether Flux is quiescent'''
    
    def get_eventlog(self, jobid):
        '''Get the eventlog for a job'''

    def get_formatted_id(self, job_id):
        '''Get the jobid in f58 format'''
    
    def nodelist_lookup(self, jobid):
        '''Get the nodelist for a job'''

    def submit_job(self, jobspec_json) -> int:
        ''''''
    
    def cancel_job(self, jobid):
        ''''''

    def start_job(self, msg, jobid):
        ''''''
    
    def complete_job(self, msg, jobid):
        ''''''
