from sqlshare_rest.util.db import get_backend
from sqlshare_rest.models import Query
from django.utils import timezone


def process_queue():
    filtered = Query.objects.filter(is_finished=False)

    try:
        oldest_query = filtered.order_by('id')[:1].get()
    except Query.DoesNotExist:
        return

    backend = get_backend()
    try:
        res = backend.run_query(oldest_query.sql, oldest_query.owner)
    except Exception as ex:
        oldest_query.has_error = True
        oldest_query.error = str(ex)

    oldest_query.is_finished = True
    oldest_query.date_finished = timezone.now()
    print "Finished: ", oldest_query.date_finished
    oldest_query.save()
