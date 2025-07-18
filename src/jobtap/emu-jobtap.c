#include <flux/core.h>
#include <flux/jobtap.h>

#define PLUGIN_NAME    "emu-jobtap"

// Struct to hold emulator context
struct emulator {
    flux_t *h;
    flux_msg_t *sim_req;
    flux_future_t *sched_req;
    int num_outstanding_job_starts;
};

// Allocates and returns an instance of the emulator struct
static struct emulator *emulator_create() {
    struct emulator* emu;

    // Allocate struct
    if (!(emu = calloc (1, sizeof (*emu))))
        return NULL;
    emu->sim_req = NULL;
    return emu;
    
}

void sim_ctx_destroy (struct emulator *ctx)
{
    if (ctx) {
        int saved_errno = errno;;
        if (ctx->sim_req)
            flux_msg_destroy(ctx->sim_req);
        free (ctx);
        errno = saved_errno;
    }
}

/* The final check that is run to make sure the outstanding starts are 0 and the sim 
req future has been satisfied before returning to the emulator to skip time*/
static inline bool is_quiescent (struct emulator *emu)
{
    return (emu->sched_req == NULL); //(emu->num_outstanding_job_starts == 0)
}

/* Performs a few sanity checks and responds to the emulator stating that the 
process is finished. It can be invoked through a quiescent request from the 
emulator itself or as a result of a job starting. */
static void check_and_respond_to_quiescent_req (struct emulator *emu)
{
    if (emu->sim_req == NULL || !is_quiescent (emu)) {
        if (!is_quiescent (emu))
            flux_log (emu->h, LOG_DEBUG, "bruh %d", emu->num_outstanding_job_starts );
        return;
    }

    const char *sim_payload = NULL;
    flux_msg_get_string (emu->sim_req, &sim_payload);
    flux_log (emu->h,
              LOG_DEBUG,
              "replying to sim quiescent req with (%s)",
              sim_payload);
    flux_respond (emu->h, emu->sim_req, sim_payload);
    flux_msg_destroy (emu->sim_req);
    emu->sim_req = NULL;
}

/*Called after a response is recieved to sched.quiescent. Validates the return payload*/
static void sched_quiescent_continuation(flux_future_t *f, void *arg)
{
    struct emulator *emu = arg;

    if (flux_future_get(f, NULL) < 0) {
        flux_log_error(emu->h, "sim quiescent response invalid. does the scheduler support sched.quiescent?\n");
    }

    if (emu->sim_req == NULL) {
        flux_log_error (emu->h, "%s: sim quiescent request is NULL", __FUNCTION__);
        return;
    }
    if (emu->sched_req != f) {
        flux_log_error (emu->h, "%s: stored future does not match continuation future", __FUNCTION__);
        return;
    }

    flux_log (emu->h, LOG_DEBUG, "receive quiescent from sched");
    emu->sched_req = NULL;
    check_and_respond_to_quiescent_req(emu);
    flux_future_destroy (f);
}

/*Sends a rpc request to the scheduler to check for quiescence.*/
void sim_sending_sched_request (struct emulator *emu)
{
    if (emu->sim_req == NULL) {
        // either not in a simulation, or if we are, the simulator does not yet
        // care about tracking quiescence
        return;
    }
    if (emu->sched_req != NULL) {
        // we are sending the scheduler more work/events before hearing back
        // from the previous quiescent request, destroy the future from that
        // previous request before sending a new request
        flux_future_destroy (emu->sched_req);
    }

    flux_log (emu->h, LOG_DEBUG, "sending quiescent req to scheduler");
    emu->sched_req = flux_rpc (emu->h, "sched.quiescent", NULL, 0, 0);    
    if (emu->sched_req == NULL) {
        flux_log(emu->h, LOG_DEBUG, "broke\n");
        flux_respond_error(emu->h, emu->sim_req, errno, "job-manager: sim_sending_sched_request: flux_rpc failed");

    }
    if (flux_future_then (emu->sched_req, -1, sched_quiescent_continuation, emu) < 0)
        flux_respond_error(emu->h, emu->sim_req, errno, "job-manager: sim_sending_sched_request: flux_future_then failed");
}


static void quiescent_cb (flux_t *h, flux_msg_handler_t *mh,
                          const flux_msg_t *msg, void *arg)
{
    struct emulator *emu = arg;

    flux_log (emu->h, LOG_DEBUG, "received quiescent request");
    emu->sim_req = flux_msg_copy (msg, true);
    if (emu->sim_req == NULL)
        flux_respond_error(emu->h, msg, errno, "job-manager: quiescent_cb: flux_msg_copy failed");

    // Check if the scheduler is quiesced
    sim_sending_sched_request(emu);
}

static int new_cb (flux_plugin_t *p,
                      const char *topic,
                      flux_plugin_arg_t *args,
                      void *arg)
{
    flux_t *h = flux_jobtap_get_flux(p);

    /*  Subscribe to events so we get all job.event.* callbacks */
    if (flux_jobtap_job_subscribe (p, FLUX_JOBTAP_CURRENT_JOB) < 0) {
        flux_log (h,
                  LOG_ERR,
                  "%s: jobtap_job_subscribe: %s",
                  topic,
                  strerror (errno));
        }

    return 0;
}

static int alloc_cb (flux_plugin_t *p,
                      const char *topic,
                      flux_plugin_arg_t *args,
                      void *arg)
{
    flux_t *h = flux_jobtap_get_flux(p);
    struct emulator *emu = arg;

    flux_log (emu->h, LOG_DEBUG, "emulator jobtap: alloc request recieved");
    
    emu->num_outstanding_job_starts++;

    return 0;
}

static int start_cb (flux_plugin_t *p,
                      const char *topic,
                      flux_plugin_arg_t *args,
                      void *arg)
{
    flux_t *h = flux_jobtap_get_flux(p);
    struct emulator *emu = arg;

    flux_log (emu->h, LOG_DEBUG, "emulator jobtap: start request recieved");

    emu->num_outstanding_job_starts--;
    check_and_respond_to_quiescent_req (emu);

    return 0;
}

int flux_plugin_init (flux_plugin_t *p)
{   
    struct emulator *emu;

    if (!(emu = emulator_create())) {
        return -1;
    }

    emu->h = flux_jobtap_get_flux(p);
    if (emu->h == NULL) return -1;
    flux_plugin_set_name(p,PLUGIN_NAME);

    flux_jobtap_service_register (p, "quiescent", quiescent_cb, emu);
    flux_plugin_add_handler (p, "job.new", new_cb, NULL);
    flux_plugin_add_handler (p, "job.event.alloc", alloc_cb, emu);
    flux_plugin_add_handler (p, "job.event.start", start_cb, emu);
    
    return 0;
}