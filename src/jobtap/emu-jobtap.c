#include <errno.h>
#include <string.h>
#include <stdbool.h>
#include <flux/core.h>
#include <flux/jobtap.h>
#include <jansson.h> 
#include <unistd.h>

#define PLUGIN_NAME "emu-jobtap"

struct emulator {
    flux_t *h;

    flux_msg_t *sim_req;

    flux_future_t *sched_req;

    /* cumulative counters */
    unsigned long long tot_new;
    unsigned long long tot_sched;     
    unsigned long long tot_alloc;     
    unsigned long long tot_start;     
    unsigned long long tot_inactive;  

    /* watermarks at last successful reply */
    unsigned long long wm_new;
    unsigned long long wm_sched;
    unsigned long long wm_alloc;
    unsigned long long wm_start;
    unsigned long long wm_inactive;

    /* expectations for current probe */
    unsigned long long exp_submits;   
    unsigned long long exp_finishes; 

    bool probe_started;        
    bool sched_quiescent_ok;  
    
    u_int64_t timestep;

    unsigned long long alloc_needed;
    json_t *buf_jobids;              
    flux_future_t *flush_req;       
};

/* helpers */

static struct emulator *emulator_create(void) {
    return calloc(1, sizeof(struct emulator));
}

static void emulator_destroy(struct emulator *emu) {
    if (!emu) return;
    int saved = errno;

    if (emu->sched_req) flux_future_destroy(emu->sched_req);
    if (emu->sim_req)   flux_msg_destroy(emu->sim_req);
    if (emu->flush_req) flux_future_destroy(emu->flush_req);
    if (emu->buf_jobids) json_decref(emu->buf_jobids);

    free(emu);
    errno = saved;
}

// Function to reset the probe back to its original state
static void emulator_reset(struct emulator *emu, bool keep_timestep)
{
    if (!emu) return;

    // Cancel any outstanding request cleanly.
    if (emu->sim_req) {
        flux_respond_error(emu->h, emu->sim_req, ECANCELED,
                           "emu-jobtap: reset while a probe was pending");
        flux_msg_destroy(emu->sim_req);
        emu->sim_req = NULL;
    }
    if (emu->sched_req) {
        flux_future_destroy(emu->sched_req);
        emu->sched_req = NULL;
    }
    if (emu->flush_req) {
        flux_future_destroy(emu->flush_req);
        emu->flush_req = NULL;
    }
    if (emu->buf_jobids) {
        json_decref(emu->buf_jobids);
        emu->buf_jobids = NULL;
    }

    // Wipe all counters and watermarks.
    emu->tot_new = emu->tot_sched = emu->tot_alloc = emu->tot_start = emu->tot_inactive = 0ULL;
    emu->wm_new  = emu->wm_sched  = emu->wm_alloc  = emu->wm_start  = emu->wm_inactive  = 0ULL;

    // Clear expectations and runtime flags.
    emu->exp_submits = 0ULL;
    emu->exp_finishes = 0ULL;
    emu->alloc_needed = 0ULL;
    emu->probe_started = false;
    emu->sched_quiescent_ok = false;

    if (!keep_timestep)
        emu->timestep = 0;

    flux_log(emu->h, LOG_INFO, "emu-jobtap: probe state reset (keep_timestep=%d)",
             keep_timestep ? 1 : 0);
}

static inline unsigned long long delta(unsigned long long total,
                                       unsigned long long wm)
{
    return (total >= wm) ? (total - wm) : 0ULL;
}

static void log_snapshot(struct emulator *emu, const char *where)
{
    if (!emu) return;

    unsigned long long d_sched    = delta(emu->tot_sched,    emu->wm_sched);
    unsigned long long d_alloc    = delta(emu->tot_alloc,    emu->wm_alloc);
    unsigned long long d_start    = delta(emu->tot_start,    emu->wm_start);
    unsigned long long d_inactive = delta(emu->tot_inactive, emu->wm_inactive);

    unsigned long long rem_sched    = (emu->exp_submits  > d_sched)    ? (emu->exp_submits  - d_sched)    : 0ULL;
    unsigned long long rem_inactive = (emu->exp_finishes > d_inactive) ? (emu->exp_finishes - d_inactive) : 0ULL;
    unsigned long long rem_alloc2start = (d_alloc > d_start) ? (d_alloc - d_start) : 0ULL;

    flux_log(emu->h, LOG_DEBUG,
        "[%s] totals: new=%llu sched=%llu alloc=%llu start=%llu inactive=%llu | "
        "wm: sched=%llu alloc=%llu start=%llu inactive=%llu | "
        "delta: sched=%llu alloc=%llu start=%llu inactive=%llu | "
        "expect: submits=%llu finishes=%llu | "
        "remaining: sched=%llu inactive=%llu alloc->start=%llu | "
        "probe_started=%d sched_quiescent_ok=%d",
        where,
        (unsigned long long)emu->tot_new,
        (unsigned long long)emu->tot_sched,
        (unsigned long long)emu->tot_alloc,
        (unsigned long long)emu->tot_start,
        (unsigned long long)emu->tot_inactive,
        (unsigned long long)emu->wm_sched,
        (unsigned long long)emu->wm_alloc,
        (unsigned long long)emu->wm_start,
        (unsigned long long)emu->wm_inactive,
        (unsigned long long)d_sched,
        (unsigned long long)d_alloc,
        (unsigned long long)d_start,
        (unsigned long long)d_inactive,
        (unsigned long long)emu->exp_submits,
        (unsigned long long)emu->exp_finishes,
        (unsigned long long)rem_sched,
        (unsigned long long)rem_inactive,
        (unsigned long long)rem_alloc2start,
        emu->probe_started ? 1 : 0,
        emu->sched_quiescent_ok ? 1 : 0
    );
}

static void flush_starts_continuation (flux_future_t *f, void *arg)
{
    struct emulator *emu = arg;
    if (!emu) {
        flux_future_destroy(f);
        return;
    }

    if (flux_rpc_get(f, NULL) < 0) {
        flux_log_error(emu->h, "flush-starts failed: %s",
                       flux_future_error_string(f));
    } else {
        flux_log(emu->h, LOG_DEBUG, "flush-starts acked");
    }

    flux_future_destroy(f);
    emu->flush_req = NULL;

}


static void try_flush_batched_starts(struct emulator *emu)
{
    if (!emu || !emu->sched_quiescent_ok) return;
    if (emu->flush_req) return; 

    unsigned long long started_since_wm = delta(emu->tot_start, emu->wm_start);
    unsigned long long need = (emu->alloc_needed > started_since_wm)
                            ? (emu->alloc_needed - started_since_wm) : 0ULL;
    if (need == 0) return;

    size_t have = emu->buf_jobids ? json_array_size(emu->buf_jobids) : 0;
    if (have < need) {

        return;
    }

    // Build payload: first `need` jobids
    json_t *root = json_object();
    json_t *arr  = json_array();
    for (size_t i = 0; i < need; ++i) {
        json_t *jid = json_array_get(emu->buf_jobids, 0); 
        if (!jid) break;
        json_array_append(arr, jid);
        json_array_remove(emu->buf_jobids, 0);
    }
    json_object_set_new(root, "jobids", arr);

    char *s = json_dumps(root, 0);
    json_decref(root);
    if (!s) return;

    emu->flush_req = flux_rpc(emu->h, "sim-exec.flush-starts", s, 0, 0);
    free(s);
    if (!emu->flush_req) {
        flux_log_error(emu->h, "flush-starts rpc failed");
        return;
    }

    if (flux_future_then(emu->flush_req, -1, flush_starts_continuation, emu) < 0) {
        flux_log_error(emu->h, "future_then failed");
        flux_future_destroy(emu->flush_req);
        emu->flush_req = NULL;
        return;
    }
}


/* Pre-probe gating: ONLY these two before calling scheduler */
static bool preprobe_expectations_met(struct emulator *emu) {
    unsigned long long d_sched    = delta(emu->tot_sched,    emu->wm_sched);
    unsigned long long d_inactive = delta(emu->tot_inactive, emu->wm_inactive);
    bool ok_sched    = (d_sched    >= emu->exp_submits);
    bool ok_inactive = (d_inactive >= emu->exp_finishes);

    flux_log(emu->h, LOG_DEBUG,
        "preprobe_check: d_sched=%llu need=%llu, d_inactive=%llu need=%llu => %s",
        (unsigned long long)d_sched,    (unsigned long long)emu->exp_submits,
        (unsigned long long)d_inactive, (unsigned long long)emu->exp_finishes,
        (ok_sched && ok_inactive) ? "MET" : "WAITING");

    return ok_sched && ok_inactive;
}

/* Post-probe gating: wait until we have seen at least alloc_needed starts
 * since the last watermark. 
 */
static bool postprobe_alloc_to_start_met(struct emulator *emu) {
    unsigned long long d_start = delta(emu->tot_start, emu->wm_start);
    bool ok = (d_start >= emu->alloc_needed);
    flux_log(emu->h, LOG_DEBUG,
             "postprobe_check starts: d_start=%llu need=%llu => %s",
             (unsigned long long)d_start,
             (unsigned long long)emu->alloc_needed,
             ok ? "MET" : "WAITING");
    return ok;
}

static void reply_and_advance(struct emulator *emu) {
    if (!emu->sim_req) return;
    const char *echo = NULL;
    (void)flux_msg_get_string(emu->sim_req, &echo);

    log_snapshot(emu, "reply_and_advance(before)");

    if (flux_respond(emu->h, emu->sim_req, echo ? echo : "{}") < 0)
        flux_log_error(emu->h, "flux_respond");

    flux_msg_destroy(emu->sim_req);
    emu->sim_req = NULL;

    /* advance watermarks to totals */
    emu->wm_new      = emu->tot_new;
    emu->wm_sched    = emu->tot_sched;
    emu->wm_alloc    = emu->tot_alloc;
    emu->wm_start    = emu->tot_start;
    emu->wm_inactive = emu->tot_inactive;

    emu->exp_submits       = 0;
    emu->exp_finishes      = 0;
    emu->probe_started     = false;
    emu->sched_quiescent_ok = false;
    
    emu->alloc_needed       = 0ULL; 

    log_snapshot(emu, "reply_and_advance(after)");
}

/* sched.quiescent handling */

static void maybe_finish_after_quiescence(struct emulator *emu)
{
    if (!emu || !emu->sim_req) return;
    if (!emu->sched_quiescent_ok) return;

    log_snapshot(emu, "maybe_finish_after_quiescence(check)");

    if (postprobe_alloc_to_start_met(emu)) {
        flux_log(emu->h, LOG_DEBUG, "postprobe guard met; replying to emulator");
        reply_and_advance(emu);
    } else {
        unsigned long long d_start = delta(emu->tot_start, emu->wm_start);
        unsigned long long outstanding = (emu->alloc_needed > d_start)
                                         ? (emu->alloc_needed - d_start) : 0ULL;
        flux_log(emu->h, LOG_DEBUG,
                 "waiting (starts): outstanding=%llu",
                 (unsigned long long)outstanding);
    }
}

static void sched_quiescent_continuation(flux_future_t *f, void *arg)
{
    struct emulator *emu = arg;
    if (!emu) return;

    if (emu->sched_req != f) {
        flux_log_error(emu->h, "%s: future mismatch", __FUNCTION__);
        flux_future_destroy(f);
        return;
    }

    const char *s = NULL;
    if (flux_rpc_get(f, &s) < 0 || !s) {
        flux_log_error(emu->h, "sched.quiescent failed/unsupported: %s",
                       flux_future_error_string(f));
        emu->sched_quiescent_ok = false;
        emu->alloc_needed = 0ULL;
    } else {
        json_error_t jerr;
        json_t *root = json_loads(s, 0, &jerr);
        if (!root) {
            flux_log(emu->h, LOG_WARNING,
                     "sched.quiescent: bad JSON payload: %s (%d:%d); payload starts with: '%c'",
                     jerr.text, jerr.line, jerr.column, s[0]);
            emu->sched_quiescent_ok = false;
            emu->alloc_needed = 0ULL;
        } else {
            int status = 1;
            unsigned long long alloc_current = 0ULL;

            json_t *js = json_object_get(root, "status");
            if (js && json_is_integer(js))
                status = (int)json_integer_value(js);

            json_t *ja = json_object_get(root, "alloc_current");
            if (ja && json_is_integer(ja))
                alloc_current = (unsigned long long)json_integer_value(ja);

            emu->sched_quiescent_ok = (status == 0);

            // Number of jobs running at the last watermark
            unsigned long long active_at_wm =
                (emu->wm_start >= emu->wm_inactive)
                    ? (emu->wm_start - emu->wm_inactive)
                    : 0ULL;

            // Number of jobs that finished since the last watermark
            unsigned long long finishes_this_step =
                (emu->tot_inactive >= emu->wm_inactive)
                    ? (emu->tot_inactive - emu->wm_inactive)
                    : 0ULL;

            // Jobs still running now that were already active last step 
            unsigned long long carryover =
                (active_at_wm > finishes_this_step)
                    ? (active_at_wm - finishes_this_step)
                    : 0ULL;

            // New allocations needed this step 
            emu->alloc_needed =
                (alloc_current > carryover)
                    ? (alloc_current - carryover)
                    : 0ULL;

            flux_log(emu->h, LOG_DEBUG,
                     "sched.quiescent: alloc_current=%llu active_at_wm=%llu finishes=%llu "
                     "carryover=%llu alloc_needed=%llu",
                     (unsigned long long)alloc_current,
                     (unsigned long long)active_at_wm,
                     (unsigned long long)finishes_this_step,
                     (unsigned long long)carryover,
                     (unsigned long long)emu->alloc_needed);

            try_flush_batched_starts(emu);

            flux_log(emu->h, LOG_DEBUG,
                     "sched_quiescent_continuation: status=%d alloc_current=%llu",
                     status, (unsigned long long)alloc_current);

            json_decref(root);

            if (emu->sched_quiescent_ok) {
                log_snapshot(emu, "sched_quiescent_continuation");
                maybe_finish_after_quiescence(emu);
            }
        }
    }

    flux_future_destroy(f);
    emu->sched_req = NULL;
    emu->probe_started = false;
}


static void send_sched_quiescent(struct emulator *emu) {
    if (!emu || !emu->sim_req) return;

    if (emu->sched_req) {
        flux_future_destroy(emu->sched_req);
        emu->sched_req = NULL;
    }

    flux_log(emu->h, LOG_DEBUG, "sending sched.quiescent");
    log_snapshot(emu, "before_sched_quiescent_rpc");

    emu->sched_req = flux_rpc(emu->h, "sched.quiescent", NULL, 0, 0);
    if (!emu->sched_req) {
        flux_log_error(emu->h, "sched.quiescent rpc failed");
        flux_respond_error(emu->h, emu->sim_req, errno,
                           "emu-jobtap: sched.quiescent rpc failed");
        flux_msg_destroy(emu->sim_req);
        emu->sim_req = NULL;
        emu->probe_started = false;
        emu->sched_quiescent_ok = false;
        return;
    }

    if (flux_future_then(emu->sched_req, -1, sched_quiescent_continuation, emu) < 0) {
        flux_log_error(emu->h, "future_then failed");
        flux_future_destroy(emu->sched_req);
        emu->sched_req = NULL;

        flux_respond_error(emu->h, emu->sim_req, errno,
                           "emu-jobtap: future_then failed");
        flux_msg_destroy(emu->sim_req);
        emu->sim_req = NULL;
        emu->probe_started = false;
        emu->sched_quiescent_ok = false;
        return;
    }

    emu->probe_started = true;
}

/* Only pre-probe gating here (sched/inactive); alloc->start is post-probe */
static void maybe_start_probe(struct emulator *emu) {
    if (!emu || !emu->sim_req) return;
    if (emu->probe_started || emu->sched_quiescent_ok) return;

    log_snapshot(emu, "maybe_start_probe(check)");

    if (!preprobe_expectations_met(emu)) {
        unsigned long long d_sched    = delta(emu->tot_sched,    emu->wm_sched);
        unsigned long long d_inactive = delta(emu->tot_inactive, emu->wm_inactive);
        flux_log(emu->h, LOG_DEBUG,
                 "waiting (preprobe): remaining sched=%lld inactive=%lld",
                 (long long)((emu->exp_submits  > d_sched)    ? (emu->exp_submits  - d_sched)    : 0),
                 (long long)((emu->exp_finishes > d_inactive) ? (emu->exp_finishes - d_inactive) : 0));
        return;
    }

    flux_log(emu->h, LOG_DEBUG, "preprobe thresholds met; probing scheduler");
    send_sched_quiescent(emu);
}

/* emulator RPC: job-manager.emu-jobtap.quiescent */

static void quiescent_cb(flux_t *h, flux_msg_handler_t *mh,
                         const flux_msg_t *msg, void *arg)
{
    struct emulator *emu = arg;
    (void)h; (void)mh;
    if (!emu) return;

    flux_log(emu->h, LOG_DEBUG, "received emulator quiescent probe");

    if (emu->sim_req) {
        flux_log(emu->h, LOG_WARNING, "replacing outstanding emulator request");
        flux_msg_destroy(emu->sim_req);
        emu->sim_req = NULL;
    }
    emu->sim_req = flux_msg_copy(msg, true);
    if (!emu->sim_req) {
        flux_respond_error(emu->h, msg, errno, "emu-jobtap: flux_msg_copy failed");
        return;
    }

    const char *payload = NULL;
    (void)flux_msg_get_string(msg, &payload);

    emu->exp_submits        = 0;
    emu->exp_finishes       = 0;
    emu->probe_started      = false;
    emu->sched_quiescent_ok = false;

    if (payload && *payload) {
        flux_log(emu->h, LOG_DEBUG, "<------------------------- TIMESTEP %ld ------------------------->", emu->timestep);
        json_error_t jerr;
        json_t *root = json_loads(payload, 0, &jerr);
        if (!root) {
            flux_log(emu->h, LOG_WARNING, "bad JSON payload: %s (%d:%d)",
                     jerr.text, jerr.line, jerr.column);
        } else {
            json_t *expect = json_object_get(root, "expect");
            if (expect && json_is_object(expect)) {
                json_t *jS = json_object_get(expect, "submits");
                json_t *jF = json_object_get(expect, "finishes");
                if (jS && json_is_integer(jS))
                    emu->exp_submits  = (unsigned long long)json_integer_value(jS);
                if (jF && json_is_integer(jF))
                    emu->exp_finishes = (unsigned long long)json_integer_value(jF);
            }
            json_decref(root);
        }
        emu->timestep += 1;
    }

    flux_log(emu->h, LOG_DEBUG, "expect: submits=%llu finishes=%llu",
             (unsigned long long)emu->exp_submits,
             (unsigned long long)emu->exp_finishes);

    /* Snapshot at probe time */
    log_snapshot(emu, "probe_arrival");

    maybe_start_probe(emu);
}

/* jobtap state/event subscriptions (counters) */

/* job.new: subscribe to this specific job AND count it */
static int cb_job_new (flux_plugin_t *p, const char *topic,
                       flux_plugin_arg_t *args, void *arg)
{
    struct emulator *emu = arg;
    flux_t *h = flux_jobtap_get_flux(p);
    if (!emu) return 0;

    if (flux_jobtap_job_subscribe(p, FLUX_JOBTAP_CURRENT_JOB) < 0) {
        flux_log(h, LOG_ERR, "%s: jobtap_job_subscribe: %s",
                 topic, strerror(errno));
    }

    emu->tot_new++;
    if (emu->sim_req) log_snapshot(emu, "job.new");
    return 0;
}

/* STATE: job.state.sched */
static int cb_job_state_sched (flux_plugin_t *p, const char *topic,
                               flux_plugin_arg_t *args, void *arg)
{
    struct emulator *emu = arg;
    if (!emu) return 0;
    emu->tot_sched++;
    if (emu->sim_req) log_snapshot(emu, "job.state.sched");

    /* Could trigger pre-probe thresholds */
    maybe_start_probe(emu);
    return 0;
}

/* EVENT: job.event.alloc */
static int cb_job_event_alloc (flux_plugin_t *p, const char *topic,
                               flux_plugin_arg_t *args, void *arg)
{
    struct emulator *emu = arg;
    if (!emu) return 0;
    emu->tot_alloc++;
    if (emu->sim_req) {
        log_snapshot(emu, "job.event.alloc");
        /* If scheduler is already quiescent, see if alloc->start just became satisfied */
        maybe_finish_after_quiescence(emu);
    }
    return 0;
}

/* EVENT: job.event.start */
static int cb_job_event_start (flux_plugin_t *p, const char *topic,
                               flux_plugin_arg_t *args, void *arg)
{
    struct emulator *emu = arg;
    if (!emu) return 0;
    emu->tot_start++;
    if (emu->sim_req) {
        log_snapshot(emu, "job.event.start");
        /* If we already got sched quiescent, this might unblock reply */
        maybe_finish_after_quiescence(emu);
    }
    return 0;
}

/* STATE: job.state.inactive */
static int cb_job_state_inactive (flux_plugin_t *p, const char *topic,
                                  flux_plugin_arg_t *args, void *arg)
{
    struct emulator *emu = arg;
    if (!emu) return 0;
    emu->tot_inactive++;
    if (emu->sim_req) log_snapshot(emu, "job.state.inactive");
    /* Could trigger pre-probe thresholds */
    maybe_start_probe(emu);
    return 0;
}

static void buffer_start_cb(flux_t *h, flux_msg_handler_t *mh,
                            const flux_msg_t *msg, void *arg)
{
    struct emulator *emu = arg;
    const char *payload = NULL;
    (void)flux_msg_get_string(msg, &payload);

    json_error_t jerr;
    json_t *root = json_loads(payload ? payload : "{}", 0, &jerr);
    if (!root) {
        flux_respond_error(h, msg, EINVAL, "bad JSON");
        return;
    }
    json_t *jid = json_object_get(root, "jobid");
    if (!jid || !json_is_integer(jid)) {
        json_decref(root);
        flux_respond_error(h, msg, EINVAL, "missing jobid");
        return;
    }

    if (!emu->buf_jobids)
        emu->buf_jobids = json_array();
    json_array_append_new(emu->buf_jobids, json_integer(json_integer_value(jid)));
    json_decref(root);

    flux_respond(h, msg, "{}");

  
    try_flush_batched_starts(emu);
}

static void reset_cb(flux_t *h, flux_msg_handler_t *mh,
                     const flux_msg_t *msg, void *arg)
{
    struct emulator *emu = arg;
    (void)mh;
    bool keep_timestep = false;

    // Optional JSON payload: {"keep_timestep": true|false}
    const char *payload = NULL;
    (void)flux_msg_get_string(msg, &payload);
    if (payload && *payload) {
        json_error_t jerr;
        json_t *root = json_loads(payload, 0, &jerr);
        if (root) {
            json_t *kt = json_object_get(root, "keep_timestep");
            if (kt && json_is_boolean(kt))
                keep_timestep = json_boolean_value(kt);
            json_decref(root);
        } else {
            // If payload is bad, just ignore and proceed with defaults.
            flux_log(h, LOG_WARNING, "reset: bad JSON payload; using defaults");
        }
    }

    emulator_reset(emu, keep_timestep);
    flux_respond(h, msg, "{}");
}

// plugin init 

int flux_plugin_init (flux_plugin_t *p)
{
    struct emulator *emu = emulator_create();
    if (!emu) return -1;

    emu->timestep = 0;
    emu->h = flux_jobtap_get_flux(p);
    if (!emu->h) {
        emulator_destroy(emu);
        return -1;
    }

    flux_plugin_set_name(p, PLUGIN_NAME);

    // RPC service exposed to emulator 
    flux_jobtap_service_register(p, "quiescent", quiescent_cb, emu);

    // Register handlers (subscription happens inside cb_job_new) 
    flux_plugin_add_handler(p, "job.new",            cb_job_new,            emu);
    flux_plugin_add_handler(p, "job.state.sched",    cb_job_state_sched,    emu);
    flux_plugin_add_handler(p, "job.event.alloc",    cb_job_event_alloc,    emu);
    flux_plugin_add_handler(p, "job.event.start",    cb_job_event_start,    emu);
    flux_plugin_add_handler(p, "job.state.inactive", cb_job_state_inactive, emu);
    flux_jobtap_service_register(p, "buffer-start", buffer_start_cb, emu);
    flux_jobtap_service_register(p, "reset", reset_cb, emu);

    return 0;
}
