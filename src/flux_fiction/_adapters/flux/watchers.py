import logging
logger = logging.getLogger(__name__)
import flux

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