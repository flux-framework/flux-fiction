# notes: I moved queue wait time into Job
from collections import namedtuple
from typing import Sequence
import math
import re 
from datetime import datetime, timedelta
import csv
from abc import ABC, abstractmethod
import logging
import time
import json
from flux_fiction._adapters.base import Adapter 

logger = logging.getLogger(__name__)

TIME_QUANTUM = 1e-6  

def qtime(t) -> float:
    return round(float(t) / TIME_QUANTUM) * TIME_QUANTUM

_event_seq_counter = 0

def make_tagged_cb(kind, job, fn, time_value):
    """Wrap a callback so we can log its kind and identity later."""
    global _event_seq_counter
    seq = _event_seq_counter
    _event_seq_counter += 1

    def cb():
        return fn()

    # attach debug metadata to the function object
    cb._ev_kind = kind                    
    cb._ev_time = time_value
    cb._ev_seq_add = seq                    
    cb._ev_jobid = getattr(job, "jobid", None) if job else None
    cb._ev_trace_idx = getattr(job, "trace_index", None) if job else None
    return cb

def create_resource(res_type, count, with_child=None):
    '''
    Creates a resource dictionary for a Job

    Note: 'count' variable must be of type int. Otherwise it will cause issues during scheduling. 
    '''
    assert isinstance(count, int) and count > 0
    res = {"type": res_type, "count": count}
    if with_child:
        assert isinstance(with_child, Sequence) and not isinstance(with_child, str)
        res["with"] = list(with_child)
    return res


def create_slot(label, count, with_child):
    '''
    Helper function for creating the slot section of a jobspec for a Job
    '''
    slot = create_resource("slot", math.ceil(count), with_child or [])
    slot["label"] = label
    return slot

class Job(object):
    '''
    Class to track individual jobs within the emulator
    '''
    def __init__(self, nnodes, ncpus, submit_time, elapsed_time, timelimit, exitcode=0, ngpus=0):
        self.nnodes = nnodes
        self.exclusive = False
        self.cores_per_node = None
        self.gpus_per_node = None
        self.ncpus = ncpus
        self.ngpus = int(ngpus or 0)
        self.submit_time = submit_time
        self.elapsed_time = elapsed_time
        self.timelimit = timelimit
        self.exitcode = exitcode
        self.start_time = None
        self.state_transitions = {}
        self._jobid = None
        self._jobspec = None
        self._submit_future = None
        self.trace_index = None     # set from reader order (see below)
        self.real_submit = None     # time.time() at actual submit()
        self.real_start  = None     # time.time() when sim_exec.start processed
        self.real_finish = None     # time.time() when complete_job() runs

    # @property
    # def jobspec(self):
    #     if self._jobspec is not None:
    #         return self._jobspec

    #     # ------------------------------------------------------------
    #     # Build resources under the slot. If your R is node->socket->core
    #     # (and maybe gpu), the jobspec should include a socket layer too.
    #     # ------------------------------------------------------------

    #     if self.exclusive:
    #         # request full node capacity
    #         if self.cores_per_node is None:
    #             raise ValueError("exclusive=True but cores_per_node is not set")
    #         total_cores = int(self.cores_per_node)

    #         total_gpus = 0
    #         if self.gpus_per_node:
    #             total_gpus = int(self.gpus_per_node)
    #     else:
    #         assert self.ncpus % self.nnodes == 0
    #         total_cores = math.ceil(self.ncpus / self.nnodes)

    #         total_gpus = 0
    #         if self.ngpus:
    #             if self.ngpus % self.nnodes != 0:
    #                 logger.warning(
    #                     "NGPUS ({}) not divisible by NNodes ({}); rounding up per-node request"
    #                     .format(self.ngpus, self.nnodes)
    #                 )
    #             total_gpus = math.ceil(self.ngpus / self.nnodes)

    #     # Match your tiny JGF assumption: 2 sockets per node.
    #     # (Make this configurable later if you want.)
    #     sockets_per_node = 2

    #     # Distribute per-node requirements across sockets so we don't under-request.
    #     cores_per_socket = math.ceil(total_cores / sockets_per_node)
    #     gpus_per_socket  = math.ceil(total_gpus / sockets_per_node) if total_gpus else 0

    #     # What each socket should contain
    #     socket_with = [create_resource("core", int(cores_per_socket))]
    #     if gpus_per_socket:
    #         socket_with.append(create_resource("gpu", int(gpus_per_socket)))

    #     # Add the socket layer (THIS is the key change)
    #     socket = create_resource("socket", int(sockets_per_node), socket_with)

    #     # Slot now contains sockets, not cores directly
    #     slot = create_slot("task", 1, [socket])

    #     # Node section unchanged
    #     resource_section = create_resource("node", self.nnodes, [slot]) if self.nnodes > 0 else slot

    #     jobspec = {
    #         "version": 1,
    #         "resources": [resource_section],
    #         "tasks": [{
    #             "command": ["command", "200"],
    #             "slot": "task",
    #             "count": {"per_slot": 1},
    #         }],
    #         "attributes": {"system": {"duration": self.timelimit}},
    #     }

    #     self._jobspec = jobspec
    #     return self._jobspec
    
    @property
    def jobspec(self):
        if self._jobspec is not None:
            return self._jobspec

        withs = []
        if self.exclusive:
            # request full node capacity
            core = create_resource("core", int(self.cores_per_node))
            withs.append(core)
            if self.gpus_per_node:
                gpu = create_resource("gpu", int(self.gpus_per_node))
                withs.append(gpu)
        else:
            assert self.ncpus % self.nnodes == 0
            core = create_resource("core", math.ceil(self.ncpus / self.nnodes))
            withs.append(core)
            if self.ngpus:
                if self.ngpus % self.nnodes != 0:
                    logger.warning("NGPUS ({}) not divisible by NNodes ({}); rounding up per-node request"
                                .format(self.ngpus, self.nnodes))
                gpu = create_resource("gpu", math.ceil(self.ngpus / self.nnodes))
                withs.append(gpu)

        slot = create_slot("task", 1, withs)
        resource_section = create_resource("node", self.nnodes, [slot]) if self.nnodes > 0 else slot
        jobspec = {
            "version": 1,
            "resources": [resource_section],
            "tasks": [{
                "command": ["command", "200"],
                "slot": "task",
                "count": {"per_slot": 1},
            }],
            "attributes": {"system": {"duration": self.timelimit}},
        }
        self._jobspec = jobspec
        return self._jobspec


    def set_exclusive(self, cores_per_node, gpus_per_node=0):
        self.exclusive = True
        self.cores_per_node = int(cores_per_node)
        self.gpus_per_node = int(gpus_per_node or 0)
        self._jobspec = None  


    def submit(self, adapter: Adapter):
        jobspec_json = json.dumps(self.jobspec)
        logger.log(9, jobspec_json)
        self.real_submit = time.time()          
        self._jobid = adapter.submit_job(jobspec_json)
        logger.debug("Submitted job id %s", self._jobid)


    @property
    def jobid(self):
        return self._jobid


    @property
    def complete_time(self):
        if self.start_time is None:
            raise ValueError("Job has not started yet")
        return self.start_time + self.elapsed_time

    def start(self, adapter: Adapter, start_time):
        '''
        Records the time that the job was started by Flux and tells the job manager that the request is being handled
        '''
        self.start_time = qtime(start_time)        
        adapter.ack_start(self.jobid)

    def complete(self, adapter: Adapter):
        '''
        Emits the finish and release events when a job is complete
        '''
        adapter.ack_complete(self.jobid)

    def cancel(self, adapter: Adapter):
        '''
        Emits the cancel event for a job
        '''
        return adapter.cancel_job(self.jobid)
        

    def insert_apriori_events(self, simulation):
        '''
        Adds the submit times for every job into the event list

        This defines the order in which jobs are submitted to flux 
        '''
        # TODO: add priority to `add_event` so that all submits for a given time
        # can happen consecutively, followed by the waits for the jobids
        simulation.step_expect[self.submit_time]["submits"] += 1
        cb = make_tagged_cb("submit", self, lambda: simulation.submit_job(self), self.submit_time)
        simulation.add_event(self.submit_time, cb)   # no other changes needed

    def record_state_transition(self, state, time):
        '''
        Adds the time that a job state transition occurred to a dict "state_transitions"
        '''
        self.state_transitions[state] = time

    def queue_wait_time(self) -> float:
        """Sim-time queue wait: STARTED - SUBMITTED (no runtime)."""
        sub = self.state_transitions.get("SUBMITTED", None)
        sta = self.state_transitions.get("STARTED", None)
        if sub in (None, "") or sta in (None, ""):
            return 0.0
        return max(0.0, float(sta) - float(sub))
    
def datetime_to_epoch(dt):
    return int((dt - datetime(1970, 1, 1)).total_seconds())


re_dhms = re.compile(r"^\s*(\d+)[:-](\d+):(\d+):(\d+)\s*$")
re_hms = re.compile(r"^\s*(\d+):(\d+):(\d+)\s*$")

def walltime_str_to_timedelta(walltime_str):
    (days, hours, mins, secs) = (0, 0, 0, 0)
    match = re_dhms.search(walltime_str)
    if match:
        days = int(match.group(1))
        hours = int(match.group(2))
        mins = int(match.group(3))
        secs = int(match.group(4))
    else:
        match = re_hms.search(walltime_str)
        if match:
            hours = int(match.group(1))
            mins = int(match.group(2))
            secs = int(match.group(3))
    return timedelta(days=days, hours=hours, minutes=mins, seconds=secs)


class JobTraceReader(ABC):
    '''
    Class that is used to ingest job traces
    '''
    def __init__(self, tracefile):
        self.tracefile = tracefile

    @abstractmethod
    def validate_trace(self):
        pass

    @abstractmethod
    def read_trace(self):
        pass


def job_from_slurm_row(row):
    '''
    generates a Job class from a sacct style job trace
    '''
    kwargs = {}
    if "ExitCode" in row and row["ExitCode"]:
        try:
            kwargs["exitcode"] = int(str(row["ExitCode"]).split(":")[0])
        except Exception:
            kwargs["exitcode"] = 0

    submit_time = qtime(datetime_to_epoch(datetime.strptime(row["Submit"], "%Y-%m-%dT%H:%M:%S")))

    elapsed = walltime_str_to_timedelta(row["Elapsed"]).total_seconds()
    if elapsed <= 0:
        logger.warning("Elapsed time ({}) <= 0".format(elapsed))
    timelimit = walltime_str_to_timedelta(row["Timelimit"]).total_seconds()
    if elapsed > timelimit:
        logger.warning(
            "Elapsed time ({}) greater than Timelimit ({})".format(
                elapsed, timelimit)
        )
    nnodes = int(row["NNodes"])
    ncpus = int(row["NCPUS"])
    if nnodes > ncpus:
        logger.warning(
            "Number of Nodes ({}) greater than Number of CPUs ({}), setting NCPUS = NNodes".format(
                nnodes, ncpus
            )
        )
        ncpus = nnodes
    elif ncpus % nnodes != 0:
        old_ncpus = ncpus
        ncpus = math.ceil(ncpus / nnodes) * nnodes
        logger.warning(
            "Number of Nodes ({}) does not evenly divide the Number of CPUs ({}), setting NCPUS to an integer multiple of the number of nodes ({})".format(
                nnodes, old_ncpus, ncpus
            )
        )

    ngpus = 0
    if "NGPUS" in row and row["NGPUS"] not in (None, "", "0"):
        try:
            ngpus = int(row["NGPUS"])
        except Exception:
            logger.warning("Invalid NGPUS value '{}'; treating as 0".format(row["NGPUS"]))
            ngpus = 0

    return Job(nnodes, ncpus, submit_time, elapsed, timelimit, ngpus=ngpus, **kwargs)


class SacctReader(JobTraceReader):
    required_fields_base = ["Elapsed", "Timelimit", "Submit", "NNodes", "NCPUS"]

    def __init__(self, tracefile, require_gpus=False):
        super(SacctReader, self).__init__(tracefile)
        self.require_gpus = require_gpus
        self.determine_delimiter()

    def determine_delimiter(self):
        """
        sacct outputs data with '|' as the delimiter by default, but ',' is a more
        common delimiter in general.  This is a simple heuristic to figure out if
        the job trace is straight from sacct or has had some post-processing
        done that converts the delimiter to a comma.
        """
        with open(self.tracefile) as infile:
            first_line = infile.readline()
        self.delim = '|' if '|' in first_line else ','

    def validate_trace(self):
        with open(self.tracefile) as infile:
            reader = csv.reader(infile, delimiter=self.delim)
            header_fields = set(next(reader))
        required_fields = list(SacctReader.required_fields_base)
        if self.require_gpus:
            required_fields.append("NGPUS")
        for req_field in required_fields:
            if req_field not in header_fields:
                raise ValueError("Job file is missing '{}'".format(req_field))

    def read_trace(self):
        """
        You can obtain the necessary information from the sacct command using the -o flag.
        For example: sacct -o nnodes,ncpus,timelimit,state,submit,elapsed,exitcode[,ngpus]
        """
        with open(self.tracefile) as infile:
            lines = [line for line in infile.readlines()
                     if not line.startswith('#')]
            reader = csv.DictReader(lines, delimiter=self.delim)
            jobs = [job_from_slurm_row(row) for row in reader]
        return jobs
    
Makespan = namedtuple('Makespan', ['beginning', 'end'])