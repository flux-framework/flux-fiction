import logging 
from flux.job import JournalConsumer
logger = logging.getLogger(__name__)

def journal_event_cb(event):
    """Journal is INFO-ONLY now; do not mutate simulator state here."""
    if event is None:
        return
    try:
        name = event.name.lower()
        jobid = event.jobid
    except Exception:
        name = getattr(event, "name", "?")
        jobid = getattr(event, "jobid", "?")
    logger.debug("journal event: %s job=%s", name, jobid)



def setup_journal(flux_handle):
    '''
    Function to setup a consumer for job journaling using the JournalConsumer from flux.job.journal
    '''
    consumer = JournalConsumer(flux_handle, full=False)
    consumer.set_callback(journal_event_cb)
    consumer.start()

    return consumer