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
        self._total_nodes = 0
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
        self._free_nodes = self.describe_resources(cfg)["nnodes"]
        self._total_nodes = self._free_nodes

        return

    def describe_resources(self, cfg):
        return {
            "source_path": None,
            "nnodes": int(cfg.nnodes or 0),
            "cores_per_node": int(cfg.ncpus or 1),
            "gpus_per_node": int(cfg.ngpus or 0),
            "jobspec_shape": {"intermediate_types": [], "intermediate_counts": {}},
            "rabbit_storage": {},
        }

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

        def count_resources(resources, target_type, multiplier=1):
            total = 0
            for resource in resources:
                if not isinstance(resource, Mapping):
                    continue
                count = int(resource.get("count", 1))
                current_multiplier = multiplier * count
                if resource.get("type") == target_type:
                    total += current_multiplier
                children = resource.get("with", [])
                if isinstance(children, list):
                    total += count_resources(children, target_type, current_multiplier)
            return total

        nnodes = 1
        try:
            resources = jobspec_obj.get("resources", [])
            if not isinstance(resources, list):
                resources = []

            nnodes = count_resources(resources, "node") or 1
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
        events = [
            {"timestamp": 1771624596.0322015, "name": "submit", "context": {"userid": 1000, "urgency": 16, "flags": 0, "version": 1}},
            {"timestamp": 1771624596.0427759, "name": "validate"},
            {"timestamp": 1771624596.0531645, "name": "depend"},
            {"timestamp": 1771624596.0532191, "name": "priority", "context": {"priority": 16}},
        ]
        queued = any(job.jobid == jobid for job in self._queue)
        if jobid in self._running:
            events.extend([
                {"timestamp": 1771624596.0546091, "name": "alloc"},
                {"timestamp": 1771624596.239527, "name": "start"},
            ])
        elif not queued:
            events.extend([
                {"timestamp": 1771624596.0546091, "name": "alloc"},
                {"timestamp": 1771624596.239527, "name": "start"},
                {"timestamp": 1771624596.2398844, "name": "finish", "context": {"status": 0}},
                {"timestamp": 1771624596.2399106, "name": "release", "context": {"ranks": "all", "final": True}},
                {"timestamp": 1771624596.2399344, "name": "free"},
                {"timestamp": 1771624596.2399468, "name": "clean"},
            ])
        eventlog = {
            "id": jobid,
            "eventlog": "\n".join(json.dumps(event) for event in events) + "\n",
        }
        return eventlog

    def get_job_diagnostics(self, jobid):
        queued = next((job for job in self._queue if job.jobid == jobid), None)
        running = self._running.get(jobid)
        job = queued or running
        state = "running" if running else "queued" if queued else "unknown"
        return {
            "id": jobid,
            "formatted_id": self.get_formatted_id(jobid),
            "state": state,
            "nnodes": job.nnodes if job else None,
            "jobspec": job.jobspec if job else None,
            "eventlog": self.get_eventlog(jobid),
        }

    def check_jobspec_satisfiability(self, jobspec_json):
        try:
            jobspec_obj = json.loads(jobspec_json)
        except Exception as e:
            return {"error": repr(e)}

        def count_resources(resources, target_type, multiplier=1):
            total = 0
            for resource in resources:
                if not isinstance(resource, Mapping):
                    continue
                count = int(resource.get("count", 1))
                current_multiplier = multiplier * count
                if resource.get("type") == target_type:
                    total += current_multiplier
                children = resource.get("with", [])
                if isinstance(children, list):
                    total += count_resources(children, target_type, current_multiplier)
            return total

        needed = count_resources(jobspec_obj.get("resources", []), "node")
        return {
            "satisfiable": needed <= self._total_nodes,
            "node_count": needed,
            "total_nodes": self._total_nodes,
        }

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

    
