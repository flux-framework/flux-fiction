#!/usr/bin/env python

from __future__ import print_function
import argparse
import re
import csv
import math
import json
import logging
import heapq
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from collections.abc import Sequence
from collections import namedtuple, defaultdict
import math
import six
import flux
import flux.job
import flux.util
import flux.kvs
import flux.constants
from flux.core.watchers import TimerWatcher
from flux.resource import Rlist
from flux.job import JournalConsumer
import time
from typing import List, Sequence, Union
from tqdm import tqdm

import os
TIME_QUANTUM = 1e-6  

def qtime(t) -> float:
    return round(float(t) / TIME_QUANTUM) * TIME_QUANTUM

_EVENT_LOG_FILE = "event_order_log.csv"
_EVENT_LOG_HEADER_WRITTEN = False

def log_event_execution(rows):
    global _EVENT_LOG_HEADER_WRITTEN
    write_header = not _EVENT_LOG_HEADER_WRITTEN or not os.path.exists(_EVENT_LOG_FILE)
    with open(_EVENT_LOG_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "time", "idx_in_bucket", "kind", "jobid", "trace_idx",
            "insertion_seq", "real_ts"
        ])
        if write_header:
            w.writeheader()
            _EVENT_LOG_HEADER_WRITTEN = True
        for r in rows:
            w.writerow(r)

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
        self._start_msg = None
        self.trace_index = None     # set from reader order (see below)
        self.real_submit = None     # time.time() at actual submit()
        self.real_start  = None     # time.time() when sim_exec.start processed
        self.real_finish = None     # time.time() when complete_job() runs

    @property
    def jobspec(self):
        if self._jobspec is not None:
            return self._jobspec

        # ------------------------------------------------------------
        # Build resources under the slot. If your R is node->socket->core
        # (and maybe gpu), the jobspec should include a socket layer too.
        # ------------------------------------------------------------

        if self.exclusive:
            # request full node capacity
            if self.cores_per_node is None:
                raise ValueError("exclusive=True but cores_per_node is not set")
            total_cores = int(self.cores_per_node)

            total_gpus = 0
            if self.gpus_per_node:
                total_gpus = int(self.gpus_per_node)
        else:
            assert self.ncpus % self.nnodes == 0
            total_cores = math.ceil(self.ncpus / self.nnodes)

            total_gpus = 0
            if self.ngpus:
                if self.ngpus % self.nnodes != 0:
                    logger.warning(
                        "NGPUS ({}) not divisible by NNodes ({}); rounding up per-node request"
                        .format(self.ngpus, self.nnodes)
                    )
                total_gpus = math.ceil(self.ngpus / self.nnodes)

        # Match your tiny JGF assumption: 2 sockets per node.
        # (Make this configurable later if you want.)
        sockets_per_node = 2

        # Distribute per-node requirements across sockets so we don't under-request.
        cores_per_socket = math.ceil(total_cores / sockets_per_node)
        gpus_per_socket  = math.ceil(total_gpus / sockets_per_node) if total_gpus else 0

        # What each socket should contain
        socket_with = [create_resource("core", int(cores_per_socket))]
        if gpus_per_socket:
            socket_with.append(create_resource("gpu", int(gpus_per_socket)))

        # Add the socket layer (THIS is the key change)
        socket = create_resource("socket", int(sockets_per_node), socket_with)

        # Slot now contains sockets, not cores directly
        slot = create_slot("task", 1, [socket])

        # Node section unchanged
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
    
    # @property
    # def jobspec(self):
    #     if self._jobspec is not None:
    #         return self._jobspec

    #     withs = []
    #     if self.exclusive:
    #         # request full node capacity
    #         core = create_resource("core", int(self.cores_per_node))
    #         withs.append(core)
    #         if self.gpus_per_node:
    #             gpu = create_resource("gpu", int(self.gpus_per_node))
    #             withs.append(gpu)
    #     else:
    #         assert self.ncpus % self.nnodes == 0
    #         core = create_resource("core", math.ceil(self.ncpus / self.nnodes))
    #         withs.append(core)
    #         if self.ngpus:
    #             if self.ngpus % self.nnodes != 0:
    #                 logger.warning("NGPUS ({}) not divisible by NNodes ({}); rounding up per-node request"
    #                             .format(self.ngpus, self.nnodes))
    #             gpu = create_resource("gpu", math.ceil(self.ngpus / self.nnodes))
    #             withs.append(gpu)

    #     slot = create_slot("task", 1, withs)
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


    def set_exclusive(self, cores_per_node, gpus_per_node=0):
        self.exclusive = True
        self.cores_per_node = int(cores_per_node)
        self.gpus_per_node = int(gpus_per_node or 0)
        self._jobspec = None  


    def submit(self, flux_handle):
        jobspec_json = json.dumps(self.jobspec)
        logger.log(9, jobspec_json)
        self.real_submit = time.time()          
        self._jobid = flux.job.submit(flux_handle, jobspec_json)
        logger.debug("Submitted job id %s", self._jobid)


    @property
    def jobid(self):
        return self._jobid


    @property
    def complete_time(self):
        if self.start_time is None:
            raise ValueError("Job has not started yet")
        return self.start_time + self.elapsed_time

    def start(self, flux_handle, start_msg, start_time):
        '''
        Records the time that the job was started by Flux and tells the job manager that the request is being handled
        '''
        self.start_time = qtime(start_time)          # quantize here
        self._start_msg = start_msg
        flux_handle.respond(self._start_msg,
                            payload={"id": self.jobid, "type": "start", "data": {}})

    def complete(self, flux_handle):
        '''
        Emits the finish and release events when a job is complete
        '''
        # TODO: emit "finish" event
        flux_handle.respond(
            self._start_msg,
            payload={"id": self.jobid, "type": "finish", "data": {"status": 0}}
        )
        # TODO: emit "done" event
        flux_handle.respond(
            self._start_msg,
            payload={"id": self.jobid, "type": "release",
                     "data": {"ranks": "all", "final": True}}
        )

    def cancel(self, flux_handle):
        '''
        Emits the cancel event for a job
        '''
        flux.job.RAW.cancel(flux_handle, self.jobid, "Canceled by emulator")

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


class EventList(six.Iterator):
    '''
    Class that is used to store all events that happen within the emulator along with the time that they will occur

    For example: the submit time for each job is added to the event list at the initialization of the emulator.

    The internal loop of the emulator will handle all events that occur at the same time. Then, it waits for some set of conditions
    to occur before executing the next set of events
    '''
    def __init__(self):
        self.time_heap = []
        self.time_map = {}    
        self._current_time = None

    def add_event(self, time, callback):
        '''
        Add an event to the event list

        Takes in a time that the event will occur and a callback function to be invoked at that time
        '''
        if self._current_time is not None and time <= self._current_time:
            logger.warning(
                "Adding a new event at a time ({}) <= the current time ({})".format(
                    time, self._current_time
                )
            )

        if time in self.time_map:
            self.time_map[time].append(callback)
        else:
            new_event_list = [callback]
            self.time_map[time] = new_event_list
            heapq.heappush(self.time_heap, (time, new_event_list))

    def __len__(self):
        return len(self.time_heap)

    def __iter__(self):
        return self

    def min(self):
        return self.time_heap[0] if self.time_heap else None

    def max(self):
        if not self.time_heap:
            return None
        time = max(self.time_map.keys())
        return self.time_map[time]

    def __next__(self):
        try:
            time, event_list = heapq.heappop(self.time_heap)
            self.time_map.pop(time)
            self._current_time = time  
            return time, event_list
        except (IndexError, KeyError):
            raise StopIteration()

def queue_wait_time(job) -> float:
    """Sim-time queue wait: STARTED - SUBMITTED (no runtime)."""
    sub = job.state_transitions.get("SUBMITTED", None)
    sta = job.state_transitions.get("STARTED", None)
    if sub in (None, "") or sta in (None, ""):
        return 0.0
    return max(0.0, float(sta) - float(sub))

class Simulation(object):
    '''
    Primary class for the emulator

    Contains functions needed to orchestrate the emulator 
    '''
    def __init__(
            self,
            flux_handle,
            event_list,
            job_map,
            submit_job_hook=None,
            start_job_hook=None,
            complete_job_hook=None,
            progress=None,
    ):
        self.event_list = event_list
        self.job_map = job_map
        self.current_time = 0
        self.flux_handle = flux_handle
        self.num_submits = 0
        self.progress = progress
        self.num_complete = 0
        self.pending_inactivations = set()
        self.job_manager_quiescent = True
        self.submit_job_hook = submit_job_hook
        self.start_job_hook = start_job_hook
        self.complete_job_hook = complete_job_hook
        self.pending_continuation = False
        self.step_expect = defaultdict(lambda: {"submits": 0, "finishes": 0})
        self.time_step = 0
        self.pending_start_msgs = {} 
        self.queue_wait = None
        self.kvs_samples = []          
        self.kvs_sample_every = 1      
        self.kvs_module_name = "content-sqlite"

    def sample_kvs_stats(self):
        """
        Record a KVS stat snapshot at current sim time.
        Stores: time, dbfile_size, object_count (if available)
        """
        try:
            st = get_module_stats_anyhow(self.flux_handle, self.kvs_module_name)
            self.kvs_samples.append({
                "time": float(self.current_time),
                "dbfile_size": int(st.get("dbfile_size", 0)),
                "object_count": int(st.get("object_count", 0)),
            })
        except Exception as e:
            # Don't crash the sim for metrics
            logger.warning("KVS sample failed at time=%s: %s", self.current_time, e)
            self.kvs_samples.append({
                "time": float(self.current_time),
                "dbfile_size": "",
                "object_count": "",
            })

    def dump_kvs_timeseries(self, out_path: str):
        fieldnames = ["time", "dbfile_size", "object_count"]
        with open(out_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for row in self.kvs_samples:
                w.writerow(row)


    def add_event(self, time, callback):
        '''
        Adds an event to the emulator's event list

        Takes in a time that the event will occur and a callback function to be invoked at that time
        '''
        self.event_list.add_event(time, callback)


    
    def submit_job(self, job):
        self.num_submits += 1
        job.record_state_transition("SUBMITTED", qtime(self.current_time))  
        if self.submit_job_hook:
            self.submit_job_hook(self, job)
        logger.debug("Submitting a new job")
        job.submit(self.flux_handle)
        self.job_map[job.jobid] = job
        logger.info("Submitted job {}".format(job.jobid))

    def start_job(self, jobid, start_msg):
        job = self.job_map[jobid]
        job.record_state_transition("STARTED", qtime(self.current_time))
        job.queue_wait = queue_wait_time(job)
        job.real_start = time.time()
        if self.start_job_hook:
            self.start_job_hook(self, job)
        job.start(self.flux_handle, start_msg, self.current_time)

        ct = qtime(job.complete_time)
        cb = make_tagged_cb("complete", job, lambda: self.complete_job(job), ct)
        self.add_event(ct, cb)                   
        self.step_expect[ct]["finishes"] += 1

    def complete_job(self, job):
        '''
        This is used to trigger the finish and release events for a job when the time to complete it is reached
        '''
        self.num_complete += 1
        t = qtime(self.current_time)
        job.record_state_transition("COMPLETED", t)
        job.record_state_transition("INACTIVE", t)
        job.real_finish = time.time() 

        if self.complete_job_hook:
            self.complete_job_hook(self, job)
        job.complete(self.flux_handle)
        # Update tqdm progress bar
        if self.progress is not None:
            self.progress.update(1)
        logger.info("Completed job {}".format(job.jobid))


    def record_job_state_transition(self, jobid, state):
        logger.log(9, "record_job_state_transition ignored (now simulator-owned): job=%s state=%s",
                jobid, state)

    
    def advance(self, *args, **kwargs):
        '''
        "Internal" loop for the emulator.

        It will process all of the events that occur and the next time in the event list

        If there are no events currently, the emulator will exit. However, if there are more jobs submitted than jobs completed
        the emulator will set pending_continuation where it will be continued when a job starts up

        Whenever all events for a specific point in time have been processed, we will check for "quiescence" or whether the 
        scheduler is idle. We wait until the scheduler is idle before proceeding in case new events are added. 

        This phase can be thought of as a "collection" phase. We collect new start events. This phase is where the emulator is
        most likely to break because it isn't possible for us to determine which jobs need to be scheduled at a specific time. 

        Currently, we wait til the scheduler is idle and then wait another 100ms to make sure that nothing else is starting up.
        This is because sometimes the scheduler will be idle for a tiny window before scheduling the next job instead of just 
        scheduling them both before becoming idle. 
        
        #TODO make this process more reliable 
        '''
        events_at_time = []  
        try:
            self.current_time, events_at_time = next(self.event_list)
        except StopIteration:
            if self.num_complete < self.num_submits:
                # Jobs in flight; ask the plugin to tell us when the system is stable.
                logger.info("Event list empty but jobs in flight; probing jobtap for quiescence")
                self.flux_handle.rpc(
                    "job-manager.emu-jobtap.quiescent",
                    payload=json.dumps({"time": self.current_time})   
                ).then(lambda fut, arg: arg.quiescent_cb(), arg=self)
                return  
            else:
                logger.info(f"completes {self.num_complete} submits {self.num_submits}")
                logger.info("No more events in event list, running post-sim analysis")
                self.post_verification()
                logger.info("Ending simulation")
                self.flux_handle.reactor_stop(self.flux_handle.get_reactor())
                return
        logger.info("Fast-forwarding time to {}".format(self.current_time))

        # record execution order exactly as it will happen
        _exec_rows = []
        for i, cb in enumerate(events_at_time):
            kind = getattr(cb, "_ev_kind", "other")
            jobid = getattr(cb, "_ev_jobid", None)
            trace_idx = getattr(cb, "_ev_trace_idx", None)
            ins_seq = getattr(cb, "_ev_seq_add", None)
            _exec_rows.append({
                "time": f"{float(self.current_time):.6f}",
                "idx_in_bucket": i,
                "kind": kind,
                "jobid": jobid,
                "trace_idx": trace_idx,
                "insertion_seq": ins_seq,
                "real_ts": f"{time.time():.6f}",
            })

        # write the snapshot for this bucket
        log_event_execution(_exec_rows)

        # run callbacks exactly once
        for cb in events_at_time:
            cb()

        # KVS sampling (time series)
        if self.kvs_sample_every and (self.time_step % self.kvs_sample_every == 0):
            self.sample_kvs_stats()

        if self.time_step == 0:
            time.sleep(0.5)
        self.time_step+=1

        # build expect and probe (no second execution loop!)
        expect = self.step_expect.get(self.current_time, {"submits": 0, "finishes": 0})
        payload = {
            "time": self.current_time,
            "expect": {
                "submits": int(expect["submits"]),
                "finishes": int(expect["finishes"]),
            },
        }
        self.flux_handle.rpc(
            "job-manager.emu-jobtap.quiescent",
            payload=json.dumps(payload)
        ).then(lambda fut, arg: arg.quiescent_cb(), arg=self)

        if self.current_time in self.step_expect:
            del self.step_expect[self.current_time]

    # def is_quiescent(self):
    #     '''
    #     Checks for some conditions that imply the system is not quiescent
    #     '''
    #     return self.job_manager_quiescent and len(self.pending_inactivations) == 0

    def quiescent_cb(self):
        '''
        Calls upon the scheduler to see if it is idle

        Will call advance if it becomes idle after waiting for 100ms 
        '''
        logger.info("Quiescent confirmed by jobtap")
        self.job_manager_quiescent = True
        self.advance()


    def post_verification(self):
        '''
        This function looks to make sure all jobs have run to completion before program ends
        If they have not, it likely means an issue with the emulator
        As a result, job event log will be output in the logger for each job that didn't complete
        '''
        for jobid, job in six.iteritems(self.job_map):
            if 'INACTIVE' not in job.state_transitions:
                logger.warning(
                    "Job {} had not reached the inactive state by simulation termination time.".format(jobid))
                eventlog = flux.job.job_kvs_lookup(
                    self.flux_handle, jobid, keys=["eventlog"])
                logger.debug(f"Job ID: {flux.job.JobID(eventlog['id']).f58}")
                lines = eventlog["eventlog"].strip().split("\n")
                for line in lines:
                    parsed = json.loads(line)
                    pretty_str = json.dumps(parsed, indent=4)
                    logger.debug(pretty_str)

    def dump_eventlog(self):
        """
        Print job eventlog to CSV.

        Many Flux eventlog entries have name="event" and the verb in "type".
        Prefer "type", with a fallback to "name".
        """
        fieldnames = [
            "jobid", "submit", "validate", "depend", "priority",
            "alloc", "start", "finish", "release", "free", "clean"
        ]

        with open("eventlog.csv", "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for jobid, job in six.iteritems(self.job_map):
                eventlog = flux.job.job_kvs_lookup(self.flux_handle, jobid, keys=["eventlog"])

                row = {"jobid": jobid}
                for event in fieldnames[1:]:
                    row[event] = ""

                lines = (eventlog.get("eventlog") or "").strip().split("\n")
                for line in lines:
                    if not line.strip():
                        continue
                    parsed = json.loads(line)

                    # Prefer "type" (common), fall back to "name" (sometimes holds the verb)
                    evt = (parsed.get("type") or parsed.get("name") or "").lower()

                    if evt in row and not row[evt]:
                        row[evt] = parsed.get("timestamp", "")

                writer.writerow(row)



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


@six.add_metaclass(ABCMeta)
class JobTraceReader(object):
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

def attach_scheduling_graph(rlist_json: dict, scheduling: dict) -> dict:
    """
    Mirror cmd_parse_config(): rl->scheduling = sched_json
    No conversion, no wrapping (unless you choose to).
    """
    if not isinstance(rlist_json, dict):
        raise TypeError("rlist_json must be a dict")
    if not isinstance(scheduling, dict):
        raise TypeError("scheduling must be a dict (parsed JSON object)")
    rlist_json["scheduling"] = scheduling
    return rlist_json


def load_json_file(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)

def insert_resource_R_from_json(flux_handle, rjson_path: str):
    """
    Load a complete RFC20 / RV1 resource description from JSON
    and install it into KVS as resource.R.

    This mirrors the C path:
      json_load_file -> rlist_from_json -> rlist_puts
    except we already trust the JSON is valid.
    """
    if not os.path.exists(rjson_path):
        raise FileNotFoundError(f"Resource JSON file not found: {rjson_path}")

    with open(rjson_path, "r") as f:
        rjson = json.load(f)

    if not isinstance(rjson, dict):
        raise ValueError("resource.R JSON must be a JSON object")

    logger.info("Loading full resource.R from %s", rjson_path)

    # Put directly into KVS
    rc = flux.kvs.put(flux_handle, "resource.R", rjson)
    if rc is not None:
        raise RuntimeError(f"flux.kvs.put(resource.R) failed, rc={rc}")

    rc = flux.kvs.commit(flux_handle)
    if rc is not None:
        raise RuntimeError(f"flux.kvs.commit() failed, rc={rc}")

def insert_resource_data(flux_handle, num_ranks, cores_per_rank,
                         hostname_pattern="node{rank}", gpus_per_rank=0,
                         scheduling_path=None, scheduling_obj=None):
    """
    Build R using Rlist.add_rank(...) for cores, then add GPU children per rank
    with rlist.add_child(rank, "gpu", "<idset>"). Debug-print the final JSON
    before committing to KVS.
    """
    if num_ranks <= 0 or cores_per_rank <= 0:
        raise ValueError("Number of ranks and cores per rank must be positive integers")

    rlist = Rlist()

    for rank in range(num_ranks):
        core_range = f"0-{cores_per_rank - 1}" if cores_per_rank > 1 else "0"
        hostname = hostname_pattern.format(rank=rank)

        # Add the node with its core children
        rlist.add_rank(rank, hostname=hostname, cores=core_range)

        # If GPUs enabled, append a gpu child ID set to this rank
        if gpus_per_rank and gpus_per_rank > 0:
            gpu_range = f"0-{gpus_per_rank - 1}" if gpus_per_rank > 1 else "0"
            rlist.add_child(rank, "gpu", gpu_range)

    # Encode to the R JSON string
    rlist_str = rlist.encode()
    try:
        rlist_json = json.loads(rlist_str)
    except Exception:
        rlist_json = {"RAW_R": rlist_str}

    logger.debug("resource.R going to KVS:\n%s",
                 json.dumps(rlist_json, indent=2, sort_keys=True))

    # Attach scheduling JSON
    if scheduling_path and scheduling_obj:
        raise ValueError("Use only one of scheduling_path or scheduling_obj")

    if scheduling_path:
        sched = load_json_file(scheduling_path)
        out = attach_scheduling_graph(rlist_json, sched)
        if out is not None:
            rlist_json = out
    elif scheduling_obj:
        out = attach_scheduling_graph(rlist_json, scheduling_obj)
        if out is not None:
            rlist_json = out

    # NOW debug-print the final thing
    print("FINAL resource.R going to KVS:\n%s",
                 json.dumps(rlist_json, indent=2, sort_keys=True))


    # Put into KVS and commit
    kvs_key = "resource.R"
    put_rc = flux.kvs.put(flux_handle, kvs_key, rlist_json)
    if put_rc is not None:
        raise ValueError(f"Error inserting resource data into KVS, rc={put_rc}")

    commit_rc = flux.kvs.commit(flux_handle)
    if commit_rc is not None:
        raise ValueError(f"Error committing resource data to KVS, rc={commit_rc}")
    
    print("Rlist ENCODE():", rlist.encode())



def get_loaded_modules(flux_handle):
    """
    Retrieve the list of loaded modules in the current Flux instance.
    """
    try:
        modules = flux_handle.rpc("module.list").get()["mods"]
        return modules
    except Exception as e:
        raise RuntimeError(f"Error retrieving loaded modules: {e}")


def load_missing_modules(flux_handle):
    # TODO: check that necessary modules are loaded
    # if not, load them
    # return an updated list of loaded modules
    # Should be checking for the jobtap module
    loaded_modules = get_loaded_modules(flux_handle)
    pass


def reload_modules(flux_handle, queue_policy = "fcfs", match_policy="first"):
    '''
    To make the resource.R that we submitted to KVS earlier register with the 
    Flux instance, we need to reload both the resource module and scheduler in 
    a specific order 

    (Sched Unload -> Res Unload -> Res Load -> Sched Load)

    It has to be in that order or the scheduler becomes confused

    scheduler parameter defines if we want to use fluxion or sched simple. Eventually, we will define policies here when reloading schedulers
    '''
    sched_module = "sched-simple"
    path = None
    resource_module_path = None
    fluxion_qmanager_path = None
    fluxion_resource_path = None
    # Acquire the path to the scheduling module being used
    # Additionally, acquire the path to the resource module
    for module in get_loaded_modules(flux_handle):
        print(module)
        if "sched-simple" in module["services"]:
            sched_module = module["name"]
            path = module["path"]
        elif "sched-fluxion-qmanager" in module["name"]:
            sched_module = "fluxion"
            fluxion_qmanager_path = module["path"]
        elif "sched-fluxion-resource" in module["name"]:
            fluxion_resource_path = module["path"]
        elif "resource" in module["name"]:
            resource_module_path = module["path"]
        elif "feasibility" in module["name"]:
            feasibility_module_path = module["path"]

    if path:
        print(f"{path}")
    elif fluxion_qmanager_path:
        print(fluxion_qmanager_path)
    logger.debug(
        "Reloading the '{}' and 'resource' module".format(sched_module))
    if  resource_module_path is not None:
        try:
            if sched_module == "sched-simple":
                flux_handle.rpc("module.remove", payload={
                                "name": "sched-simple"}).get()
            else:
                flux_handle.rpc("module.remove", payload={
                                "name": "sched-fluxion-qmanager"}).get()
                flux_handle.rpc("module.remove", payload={
                                "name": "sched-fluxion-feasibility"}).get()
                flux_handle.rpc("module.remove", payload={
                                "name": "sched-fluxion-resource"}).get()
            flux_handle.rpc("module.remove", payload={
                            "name": "resource"}).get()
            
        except Exception as e:
            logger.error(f"Error removing module: {e}")
        
        
        try:
            flux_handle.rpc("module.load",
                            payload={
                                "path": resource_module_path,
                                "args": ["noverify", "monitor-force-up"],
                            }).get()
            if sched_module == "sched-simple":
                flux_handle.rpc("module.load", payload={
                                "path": path, "args": []}).get()
            else:
                flux_handle.rpc("module.load", payload={
                                "path": fluxion_resource_path, "args": [f"match-policy={match_policy}"]}).get()
                # "queue-policy=conservative"
                flux_handle.rpc("module.load", payload={
                                "path": feasibility_module_path, "args": []}).get()
                flux_handle.rpc("module.load", payload={
                                "path": fluxion_qmanager_path, "args": [f"queue-policy={queue_policy}"]}).get()

        except Exception as e:
            logger.error(e)
    else:
        raise RuntimeError(
            "Unable to get scheduler path (is your scheduler module loaded?)")



def job_exception_cb(flux_handle, watcher, msg, cb_args):
    '''
    Placeholder for handling job excceptions
    '''
    logger.warning("Detected a job exception, but not handling it")


def sim_exec_start_cb(flux_handle, watcher, msg, simulation):
    payload = msg.payload
    jobid = payload["id"]

    # Buffer the raw start request so we can ack it later in a batch
    simulation.pending_start_msgs[jobid] = msg

    # Tell jobtap we've received this start (async; no blocking inside watcher)
    flux_handle.rpc(
        "job-manager.emu-jobtap.buffer-start",
        payload={"jobid": jobid}
    ).then(lambda fut, arg: None, arg=None)
    

def sim_exec_flush_starts_cb(flux_handle, watcher, msg, simulation):
    body = msg.payload or {}
    jobids = body.get("jobids", [])

    for jobid in jobids:
        start_msg = simulation.pending_start_msgs.pop(jobid, None)
        if start_msg is None:
            logger.error("flush-starts: unknown jobid %s", jobid)
            continue
        # Now we actually "start" the job (records start time, schedules complete, sends ACK)
        simulation.start_job(jobid, start_msg)

    # reply to jobtap so it knows we accepted the flush
    flux_handle.respond(msg, payload={"ok": True})



def exec_hello(flux_handle):
    '''
    Registers the simple exec module as the exec system in the job manager
    '''
    logger.debug("Registering sim-exec with job-manager")
    flux_handle.rpc("job-manager.exec-hello",
                    payload={"service": "sim-exec"}).get()


def service_add(f, name):
    future = f.service_register(name)
    return f.future_get(future, None)


def service_remove(f, name):
    future = f.service_unregister(name)
    return f.future_get(future, None)


def journal_event_cb(event, simulation):
    """Journal is INFO-ONLY now; do not mutate simulator state here."""
    if event is None:
        return
    try:
        name = event.name.lower()
        jobid = event.jobid
    except Exception:
        name = getattr(event, "name", "?")
        jobid = getattr(event, "jobid", "?")
    logger.log(9, "journal event: %s job=%s", name, jobid)



def setup_journal(flux_handle, simulation):
    '''
    Function to setup a consumer for job journaling using the JournalConsumer from flux.job.journal
    '''
    consumer = JournalConsumer(flux_handle, full=False)
    consumer.set_callback(journal_event_cb, simulation)
    consumer.start()

    return consumer


def setup_watchers(flux_handle, simulation):
    '''
    Adds all appropriate watchers to the emulator

    Currently, only adds one to watch for "sim-exec.start"
    '''
    watchers = []
    services = set()
    for type_mask, topic, cb, args in [
        (
            flux.constants.FLUX_MSGTYPE_REQUEST,
            "sim-exec.start",
            sim_exec_start_cb,
            simulation,
        ),
        (flux.constants.FLUX_MSGTYPE_REQUEST, "sim-exec.flush-starts",  sim_exec_flush_starts_cb, simulation),
    ]:
        watcher = flux_handle.msg_watcher_create(
            cb, type_mask=type_mask, topic_glob=topic, args=args
        )
        watcher.start()
        watchers.append(watcher)
        if type_mask == flux.constants.FLUX_MSGTYPE_REQUEST:
            service_name = topic.split(".")[0]
            if service_name not in services:
                service_add(flux_handle, service_name)
                services.add(service_name)
    return watchers, services


def teardown_watchers(flux_handle, watchers, services):
    '''
    Destructs watchers
    '''
    for watcher in watchers:
        watcher.stop()
    for service_name in services:
        service_remove(flux_handle, service_name)


Makespan = namedtuple('Makespan', ['beginning', 'end'])


class SimpleExec(object):
    '''
    This module is a simulator for job execution. It is loaded like a broker module in Flux

    The exact behavior of SimpleExec currently is to recieve start job notifications from the 
    job manager in Flux, notify the user-event simulator, mark down bookkeeping information
    about jobs, send an ackowledgment to the job manager that jobs are starting, and handle
    requests from the user-event simulator to end jobs.

    One behavior to note is that this will currently buffer the start acknowledgements for jobs
    to the job manager and flush them at the end of each timestep in the user-event simulator.
    This is planned to become togglable soon. It is more realistic to not batch the acks but 
    it is useful to make times line up properly.
    '''
    def __init__(self, num_nodes, cores_per_node, gpus_per_node=0, exclusive=False):
        self.num_nodes = num_nodes
        self.cores_per_node = cores_per_node
        self.gpus_per_node = int(gpus_per_node or 0)
        self.exclusive = bool(exclusive)
        self.num_free_nodes = num_nodes
        self.used_core_hours = 0
        self.used_gpu_hours = 0

        self.makespan = Makespan(
            beginning=float('inf'),
            end=-1,
        )

    def update_makespan(self, current_time):
        '''
        Helper function that allows you to modify the makespan
        '''
        if current_time < self.makespan.beginning:
            self.makespan = self.makespan._replace(beginning=current_time)
        if current_time > self.makespan.end:
            self.makespan = self.makespan._replace(end=current_time)

    def submit_job(self, simulation, job):
        '''
        Updates the makespan on job submission
        '''
        self.update_makespan(simulation.current_time)

    def start_job(self, simulation, job):
        '''
        Checks to make sure the job requirements are feasible for jobs that are starting

        #TODO This does not work properly when allocating less cores than an entire node
        '''
        self.num_free_nodes -= job.nnodes
        if self.num_free_nodes < 0:
            logger.error("Scheduler over-subscribed nodes")

        if not self.exclusive:
            if (job.ncpus / job.nnodes) > self.cores_per_node:
                logger.error("Scheduler over-subscribed cores on the node")
            if job.ngpus:
                if not self.gpus_per_node:
                    logger.error("Job requested GPUs but system has none configured")
                elif (job.ngpus / job.nnodes) > self.gpus_per_node:
                    logger.error("Scheduler over-subscribed GPUs on the node")

    def complete_job(self, simulation, job):
        '''
        Updates the makespan for jobs that complete
        '''
        self.num_free_nodes += job.nnodes
        if self.exclusive:
            self.used_core_hours += (self.cores_per_node * job.nnodes * job.elapsed_time) / 3600
            if self.gpus_per_node:
                self.used_gpu_hours += (self.gpus_per_node * job.nnodes * job.elapsed_time) / 3600
        else:
            self.used_core_hours += (job.ncpus * job.elapsed_time) / 3600
            if job.ngpus:
                self.used_gpu_hours += (job.ngpus * job.elapsed_time) / 3600
        self.update_makespan(simulation.current_time)

    def post_analysis(self, simulation):
        """
        Outputs statistics about the simulation whenever called.
        """
        if self.makespan.beginning > self.makespan.end:
            logger.warning(
                "Makespan beginning ({}) greater than end ({})".format(
                    self.makespan.beginning, self.makespan.end
                )
            )

        makespan_hours = max(0.0, (self.makespan.end - self.makespan.beginning) / 3600.0)

        # Core stats
        total_core_hours = self.num_nodes * self.cores_per_node * makespan_hours
        print("Makespan (hours): {:.1f}".format(makespan_hours))
        print("Total Core-Hours: {:,.1f}".format(total_core_hours))
        print("Used Core-Hours: {:,.1f}".format(self.used_core_hours))
        if total_core_hours > 0:
            print("Average Core-Utilization: {:.2f}%".format(
                (self.used_core_hours / total_core_hours) * 100.0
            ))
        else:
            print("ERROR: Total core hours is 0. Simulation likely didn't run or no jobs were submitted.")

        # GPU stats (only if GPUs are configured)
        if self.gpus_per_node:
            total_gpu_hours = self.num_nodes * self.gpus_per_node * makespan_hours
            print("Total GPU-Hours: {:,.1f}".format(total_gpu_hours))
            print("Used GPU-Hours:  {:,.1f}".format(self.used_gpu_hours))
            if total_gpu_hours > 0:
                print("Average GPU-Utilization: {:.2f}%".format(
                    (self.used_gpu_hours / total_gpu_hours) * 100.0
                ))
            else:
                print("ERROR: Total GPU hours is 0. (GPUs configured but zero makespan?)")



logger = logging.getLogger("flux-emulator")

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

import subprocess
def get_module_stats_anyhow(flux_handle, module_name: str) -> dict:
    """
    Best-effort way to get `flux module stats <name>` as a dict.
    Tries RPC first; falls back to calling the `flux` CLI.
    """
    # # 1) RPC attempt (works on many builds)
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

@flux.util.CLIMain(logger)
def main():

    # ______________________________
    # --- INITIALIZATION PHASE --- |
    # ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾

    parser = argparse.ArgumentParser()

    # The file that contains the job traces being input into the program. Should be in the sacct trace format
    parser.add_argument("job_file")

    # The number of nodes that are used for the simulated system configuration
    parser.add_argument("num_ranks", type=int)

    # The number of cores that are used per node in the simulated system configuration
    parser.add_argument("cores_per_rank", type=int)

    # The number of GPUs that are used per node in the simulated system configuration
    # Can be 3rd positional argument or specified with flags
    parser.add_argument("gpus_per_rank", nargs="?", type=int, default=0, help="(optional) GPUs per rank/node; enable GPU-aware mode if > 0")
    parser.add_argument("--gpus-per-rank", dest="gpus_per_rank", type=int, help="Override GPUs per rank/node (same as optional 3rd positional)")

    # Specifies log level for Flux
    # TODO: Logging isnt currently working. It could be because Im just not specifying a log level when i run the program. There is no default
    parser.add_argument("--log-level", type=int, help="Python logger level")
    parser.add_argument("--log-file", default="emulator.log", help="Write logs to this file (default: emulator.log)")

    # Flag to specify whether nodes will be counted as exclusive or not
    # (you only have to say the job needs 1 node and the program assumes you want all the cores and gpus in a node)
    parser.add_argument("--exclusive", action="store_true", help="Each job consumes all resources on its allocated nodes (ignore per-job CPU/GPU counts)")
    args = parser.parse_args()

    # Set log level from the input parameter 
    for h in list(logger.handlers):
        logger.removeHandler(h)

    logger.setLevel(args.log_level if args.log_level is not None else logging.INFO)
    log_file_handler = logging.FileHandler(args.log_file, mode="w")
    log_file_handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(log_file_handler)
    logger.propagate = False
    logger.info("Logger initialized; writing to %s", args.log_file)

    # Get the flux handle
    flux_handle = flux.Flux()
    kvs_size_start = get_content_sqlite_dbfile_size(flux_handle)

    # Configure our simulated exec system and the user event simulator
    exec_validator = SimpleExec(args.num_ranks, args.cores_per_rank, gpus_per_node=args.gpus_per_rank, exclusive=args.exclusive)
    simulation = Simulation(
        flux_handle,
        EventList(),
        {},
        submit_job_hook=exec_validator.submit_job,
        start_job_hook=exec_validator.start_job,
        complete_job_hook=exec_validator.complete_job,
    )

    # Take the system resource configuration and put it in the KVS 
    # insert_resource_data(flux_handle, args.num_ranks, args.cores_per_rank, gpus_per_rank=args.gpus_per_rank)
    insert_resource_data(flux_handle, args.num_ranks, args.cores_per_rank, gpus_per_rank=args.gpus_per_rank, scheduling_path="/home/j/Desktop/flux/flux-sched/t/data/resource/jgfs/tiny.json")

    
    # insert_resource_R_from_json(flux_handle, rjson_path="/home/j/Desktop/flux/sc25_poster/flux-fiction/src/python/tuolumne.json")
    #
    # TODO: add in a parameter to allow you to just specify module parameters instead of putting a 
    # function paramater for every single module paramter 
    reload_modules(flux_handle, queue_policy="conservative", match_policy="first")

    
    # Read in the job traces from the specified file and make a list of jobs
    reader = SacctReader(args.job_file, require_gpus=(args.gpus_per_rank and args.gpus_per_rank > 0))
    
    reader.validate_trace()
    jobs = list(reader.read_trace())
    

    
    # Attach a traceable index to jobs for use in identifying jobs cross-run when their ID has changed
    for idx, job in enumerate(jobs):
        job.trace_index = idx  

    # Set jobs as exclusive if applicable and insert the jobs start times into the event list in our user-event simulator    
    if args.exclusive:
        for job in jobs:
            job.set_exclusive(args.cores_per_rank, args.gpus_per_rank)
    for job in jobs:
        job.insert_apriori_events(simulation)

    # Create progress bar tracking completed jobs
    pbar = tqdm(total=len(jobs), desc="Jobs completed", unit="job", leave=True)
    simulation.progress = pbar

    # TODO: Should be checking to see if the jobtap plugin is loaded 
    load_missing_modules(flux_handle)

    # Configure RPC endpoints/watchers for our program
    watchers, services = setup_watchers(flux_handle, simulation)

    # TODO: Remove everything related to journal consumer. This is junk 
    consumer = setup_journal(flux_handle, simulation)

    # Register the exec system simulator as the exec system that we are using for Flux
    exec_hello(flux_handle)
    
    # __________________________
    # --- SIMULATION PHASE --- |
    # ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
 
    # Begin the primary event loop of the user event simulator 
    simulation.advance()

    # ________________________
    # --- POST-SIM PHASE --- |
    # ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾ 

    # Run the last set of events in the reactor. If we arent able to, we encountered an exception in our simulation
    try:
        flux_handle.reactor_run(flux_handle.get_reactor(), 0)
    except Exception as e:
        logger.error(f"Reactor encountered an exception: {e}")

    # Get rid of the watchers/services that we used in our simulation
    try:
        teardown_watchers(flux_handle, watchers, services)
    except Exception as e:
        logger.error(f"Error tearing down watchers {e}")

    if simulation.progress is not None:
        simulation.progress.close()

    # Print out the results of the simulation
    exec_validator.post_analysis(simulation)

    # I had to put this delay previously because the eventlog wasn't done being updated 
    # sometimes when we finished and we had to wait a few seconds for it to finish updating
    # Probably a better method
    time.sleep(2)

    config = f"nodes{args.num_ranks}_cpr{args.cores_per_rank}" 
    kvs_outfile = f"kvs_growth_{config}.csv"
    simulation.dump_kvs_timeseries(kvs_outfile)
    print(f"Wrote KVS time series to {kvs_outfile}")

    # Dump Flux's own eventlog
    simulation.dump_eventlog()

    # Dump out our own transition log to the file
    dump_transitions_to_csv(simulation, "job_transitions.csv", flux_handle)

    # Creates chrome traces that can be plugged into perfetto to view the occupancy graph during execution
    write_per_node_chrome_trace(simulation, "pernode.json", flux_handle)

    kvs_size_end = get_content_sqlite_dbfile_size(flux_handle)
    completed = max(1, simulation.num_complete)  

    kvs_bytes_per_completed = (kvs_size_end - kvs_size_start) / float(completed)

    makespan = max(1e-9, float(exec_validator.makespan.end - exec_validator.makespan.beginning))
    kvs_growth_bytes_per_sim_s = (kvs_size_end - kvs_size_start) / makespan

    print(f"KVS content-sqlite dbfile_size start: {kvs_size_start} bytes")
    print(f"KVS content-sqlite dbfile_size end:   {kvs_size_end} bytes")
    print(f"KVS bytes per completed job:          {kvs_bytes_per_completed:.2f} bytes/job")
    print(f"KVS growth rate:                      {kvs_growth_bytes_per_sim_s:.2f} bytes/s (sim time)")

    # Average queue wait (sim-time) across jobs that actually started
    waits = []
    for job in simulation.job_map.values():
        # queue_wait is set in Simulation.start_job(); ignore jobs that never started
        if job.queue_wait is not None:
            waits.append(float(job.queue_wait))

    if waits:
        avg_wait = sum(waits) / len(waits)
        print(f"Average queue wait time: {avg_wait:.6f} seconds (sim time) over {len(waits)} jobs")
    else:
        print("Average queue wait time: N/A (no jobs have queue_wait recorded)")

    if waits:
        print(f"Max queue wait time: {max(waits):.6f} seconds (sim time)")

   
    # Reset the emu-jobtap probe to defaults after a run
    try:
        flux_handle.rpc("job-manager.emu-jobtap.reset",
                        payload={"keep_timestep": False}).get()
        logger.debug("Reset emu-jobtap probe to defaults")
    except Exception as e:
        logger.error(f"Failed to reset emu-jobtap probe: {e}")


if __name__ == "__main__":
    main()
