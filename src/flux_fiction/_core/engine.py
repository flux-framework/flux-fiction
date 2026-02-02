#TODO decouple from flux using adapter for unit testing

from __future__ import annotations

import flux_fiction._core.errors as errors
import flux_fiction._core.models as models
import flux_fiction._core.events as events
import flux_fiction._adapters.flux.stats as flux_stats
import flux_fiction._adapters.flux.modules as flux_modules
import flux_fiction._adapters.flux.resources as flux_resources
import flux_fiction._adapters.flux.watchers as flux_watchers
import flux_fiction._adapters.flux.journal as flux_journal
import flux_fiction._outputs.filesystem_output as filesystem_output

from flux_fiction._exec.simexec import SimpleExec  

from dataclasses import dataclass
import logging
from collections import defaultdict
import csv
import time
import flux 
import json
import os
from tqdm import tqdm

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EngineResult:
    ok: bool
    message: str = ""


def run(config) -> EngineResult:
    """
    Core entrypoint. Eventually this will:
      - init Flux adapter
      - load resources
      - load traces
      - execute DES loop
      - produce artifacts/metrics
    """
    logger.log(1, f"[core] engine.run() got config: {config}")

    # Get the flux handle
    flux_handle = flux.Flux()

    # Track the KVS Size at the beginning to compare later
    kvs_size_start = flux_stats.get_content_sqlite_dbfile_size(flux_handle)

    # Configure our simulated exec system and the user event simulator
    #TODO Is this validation needed or can it be overhauled?>
    exec_validator = SimpleExec(config.nnodes, config.ncpus, gpus_per_node=config.ngpus, exclusive=config.exclusive)
    simulation = Simulation(
        flux_handle,
        events.EventList(),
        {},
        submit_job_hook=exec_validator.submit_job,
        start_job_hook=exec_validator.start_job,
        complete_job_hook=exec_validator.complete_job,
    )
    # Take the system resource configuration and put it in the KVS 
    # insert_resource_data(flux_handle, args.num_ranks, args.cores_per_rank, gpus_per_rank=args.gpus_per_rank)
    flux_resources.insert_resource_data(flux_handle, config.nnodes, config.ncpus, gpus_per_rank=config.ngpus)

    
    # insert_resource_R_from_json(flux_handle, rjson_path="/home/j/Desktop/flux/sc25_poster/flux-fiction/src/python/tuolumne.json")
    #
    # TODO: add in a parameter to allow you to just specify module parameters instead of putting a 
    # function paramater for every single module paramter 
    flux_modules.reload_modules(flux_handle, queue_policy="conservative", match_policy="first")

    
    # Read in the job traces from the specified file and make a list of jobs
    reader = models.SacctReader(config.job_traces, require_gpus=(config.ngpus and config.ngpus > 0))
    
    reader.validate_trace()
    jobs = list(reader.read_trace())
    
    # Attach a traceable index to jobs for use in identifying jobs cross-run when their ID has changed
    for idx, job in enumerate(jobs):
        job.trace_index = idx  

    # Set jobs as exclusive if applicable and insert the jobs start times into the event list in our user-event simulator    
    if config.exclusive:
        for job in jobs:
            job.set_exclusive(config.ncpus, config.ngpus)
    for job in jobs:
        job.insert_apriori_events(simulation)

    # Create progress bar tracking completed jobs
    pbar = tqdm(total=len(jobs), desc="Jobs completed", unit="job", leave=True)
    simulation.progress = pbar

    # TODO: Should be checking to see if the jobtap plugin is loaded 
    flux_modules.load_missing_modules(flux_handle)

    # Configure RPC endpoints/watchers for our program
    watchers, services = flux_watchers.setup_watchers(flux_handle, simulation)

    # TODO: Remove everything related to journal consumer. This is junk 
    consumer = flux_journal.setup_journal(flux_handle, simulation)

    # Register the exec system simulator as the exec system that we are using for Flux
    flux_watchers.exec_hello(flux_handle)
    
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
        return EngineResult(ok=False, message="Error during simulation time")


    # Get rid of the watchers/services that we used in our simulation
    try:
        flux_watchers.teardown_watchers(flux_handle, watchers, services)
    except Exception as e:
        logger.error(f"Error tearing down watchers {e}")
        return EngineResult(ok=False, message="Error tearing down watchers")

    if simulation.progress is not None:
        simulation.progress.close()

    # Print out the results of the simulation
    exec_validator.post_analysis(simulation)

    # I had to put this delay previously because the eventlog wasn't done being updated 
    # sometimes when we finished and we had to wait a few seconds for it to finish updating
    # Probably a better method
    time.sleep(2)

    config = f"nodes{config.nnodes}_cpr{config.ncpus}" 
    kvs_outfile = f"kvs_growth_{config}.csv"
    simulation.dump_kvs_timeseries(kvs_outfile)
    logger.info(f"Wrote KVS time series to {kvs_outfile}")

    # Dump Flux's own eventlog
    simulation.dump_eventlog()

    # Dump out our own transition log to the file
    filesystem_output.dump_transitions_to_csv(simulation, "job_transitions.csv", flux_handle)

    # Creates chrome traces that can be plugged into perfetto to view the occupancy graph during execution
    filesystem_output.write_per_node_chrome_trace(simulation, "pernode.json", flux_handle)

    kvs_size_end = flux_stats.get_content_sqlite_dbfile_size(flux_handle)
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
        return EngineResult(ok=False, message="Failed to reset emu-jobtap probe")

    return EngineResult(ok=True, message="Ran Successfully")

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
            st = flux_stats.get_module_stats_anyhow(self.flux_handle, self.kvs_module_name)
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
        job.record_state_transition("SUBMITTED", models.qtime(self.current_time))  
        if self.submit_job_hook:
            self.submit_job_hook(self, job)
        logger.debug("Submitting a new job")
        job.submit(self.flux_handle)
        self.job_map[job.jobid] = job
        logger.info("Submitted job {}".format(job.jobid))

    def start_job(self, jobid, start_msg):
        job = self.job_map[jobid]
        job.record_state_transition("STARTED", models.qtime(self.current_time))
        job.queue_wait = job.queue_wait_time()
        job.real_start = time.time()
        if self.start_job_hook:
            self.start_job_hook(self, job)
        job.start(self.flux_handle, start_msg, self.current_time)

        ct = models.qtime(job.complete_time)
        cb = models.make_tagged_cb("complete", job, lambda: self.complete_job(job), ct)
        self.add_event(ct, cb)                   
        self.step_expect[ct]["finishes"] += 1

    def complete_job(self, job):
        '''
        This is used to trigger the finish and release events for a job when the time to complete it is reached
        '''
        self.num_complete += 1
        t = models.qtime(self.current_time)
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
        for jobid, job in self.job_map.items():
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

            for jobid, job in self.job_map.items():
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
