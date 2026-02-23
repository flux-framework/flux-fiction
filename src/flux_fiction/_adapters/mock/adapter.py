# flux_fiction/_adapters/mock/adapter.py
from __future__ import annotations

import itertools
import logging
import json
from collections import deque
from dataclasses import dataclass
from typing import Any
from collections import deque
from collections.abc import Mapping, Callable

logger = logging.getLogger(__name__)

@dataclass
class _QueuedJob:
    jobid: int
    nnodes: int
    jobspec: Any  # whatever you pass around
    trace_idx: int | None = None

class MockAdapter:
    """
    Minimal deterministic backend:
    - FCFS scheduling on nnodes
    - batched "flush starts" happens when query_quiescent() is called
    - completion acks are no-ops (but recorded for assertions)
    """

    def __init__(self) -> None:
        self.simulation = None
        self._jobid_gen = itertools.count(1)
        self._free_nodes = 0
        self._queue: deque[_QueuedJob] = deque()
        self._running: dict[int, _QueuedJob] = {}

        self.acked_starts: list[int] = []
        self.acked_completes: list[int] = []
        self.submitted: list[int] = []
        self._nodelists: dict[int, list[int]] = {}

        self._deferred: deque[Callable[[], None]] = deque()
        self._reactor_running = False

    def open(self, simulation) -> None:
        self.simulation = simulation

    def close(self) -> None:
        return

    def install_resources(self, cfg):
        '''Register emulated resources with the resource manager'''
        self._free_nodes = cfg.nnodes

        return

    def reload_scheduler(self, cfg):
        '''Restart the scheduler and associated modules'''
        return

    def register_exec_service(self):
        '''Register the applicable emulated exec service'''
        return

    def register_job_tracking(self):
        '''Setup logging for job state changes'''
        return

    def arm_watchers(self):
        '''Setup and configure any watchers'''
        return

    def start_reactor(self) -> None:
        self._reactor_running = True
        while self._reactor_running and self._deferred:
            cb = self._deferred.popleft()
            cb()

    def stop_reactor(self) -> None:
        self._reactor_running = False

    def get_kvs_stats(self) -> dict:
        # no real KVS; return stable dummy values
        return {"dbfile_size": 0, "object_count": 0}

    def submit_job(self, jobspec) -> int:
        """
        Accepts Flux jobspec-like input (dict or JSON string).
        Extracts nnodes from resources[].type=="node".
        """
        jobid = next(self._jobid_gen)
        self.submitted.append(jobid)

        if isinstance(jobspec, str):
            try:
                jobspec_obj = json.loads(jobspec)
            except json.JSONDecodeError as e:
                raise ValueError(f"submit_job expected JSON string or dict; got invalid JSON: {e}") from e
        elif isinstance(jobspec, Mapping):
            jobspec_obj = jobspec
        else:
            raise TypeError(f"submit_job expected dict or JSON string, got {type(jobspec)!r}")

        nnodes = 1
        try:
            resources = jobspec_obj.get("resources", [])
            if not isinstance(resources, list):
                resources = []

            node_res = None
            for r in resources:
                if isinstance(r, Mapping) and r.get("type") == "node":
                    node_res = r
                    break

            if node_res is not None:
                nnodes = int(node_res.get("count", 1))
        except Exception as e:
            logger.debug("MockAdapter: failed to parse nnodes from jobspec: %r", e)
            nnodes = 1

        self._queue.append(_QueuedJob(jobid=jobid, nnodes=nnodes, jobspec=jobspec_obj))
        return jobid
    
    def cancel_job(self, jobid):
        '''Cancel the job with {jobid} job id'''
        return

    def _try_schedule(self) -> list[int]:
        """
        FCFS: start as many queued jobs as fit in free_nodes.
        Returns list of jobids that were started (to be flushed).
        """
        started: list[int] = []
        while self._queue and self._queue[0].nnodes <= self._free_nodes:
            job = self._queue.popleft()
            self._free_nodes -= job.nnodes
            self._running[job.jobid] = job

            # create a deterministic nodelist for artifacts
            # naive: allocate lowest node indices
            nodes = list(range(self._free_nodes, self._free_nodes + job.nnodes))
            self._nodelists[job.jobid] = nodes

            started.append(job.jobid)
            logger.debug(f"Started: {job.jobid}")

        return started

    def query_quiescent(self, json_string_or_payload, return_cb):
        """
        In Flux, this asks jobtap whether the system is stable and
        triggers flush-starts. In mock, we deterministically flush starts now.
        """
        if self.simulation is None:
            raise RuntimeError("MockAdapter not opened with a simulation")

        def _cont():
            # schedule starts and flush them
            jobids = self._try_schedule()
            for jid in jobids:
                self.simulation.start_job(jid)
            # now signal “quiescent confirmed”
            return_cb(None, None)

        self._deferred.append(_cont)

    def get_eventlog(self, jobid):
        '''Get the eventlog for a job'''
        eventlog = {'id': jobid, 'eventlog': '{"timestamp":1771624596.0322015,"name":"submit","context":{"userid":1000,"urgency":16,"flags":0,"version":1}}\n{"timestamp":1771624596.0427759,"name":"validate"}\n{"timestamp":1771624596.0531645,"name":"depend"}\n{"timestamp":1771624596.0532191,"name":"priority","context":{"priority":16}}\n{"timestamp":1771624596.0546091,"name":"alloc"}\n{"timestamp":1771624596.239527,"name":"start"}\n{"timestamp":1771624596.2398844,"name":"finish","context":{"status":0}}\n{"timestamp":1771624596.2399106,"name":"release","context":{"ranks":"all","final":true}}\n{"timestamp":1771624596.2399344,"name":"free"}\n{"timestamp":1771624596.2399468,"name":"clean"}\n'}
        return eventlog

    def get_formatted_id(self, job_id):
        '''Get the jobid in f58 format'''
        return f"ƒ{job_id}"

    def nodelist_lookup(self, jobid: int):
        return self._nodelists.get(jobid, []), "mock"

    def ack_start(self, jobid: int) -> None:
        self.acked_starts.append(jobid)

    def ack_complete(self, jobid: int) -> None:
        self.acked_completes.append(jobid)
        job = self._running.pop(jobid, None)
        logger.debug(f"Ended: {jobid}")
        if job is not None:
            self._free_nodes += job.nnodes

    
