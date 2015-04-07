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
        cursor = backend.run_query(oldest_query.sql,
                                   oldest_query.owner,
                                   return_cursor=True)

        name = "query_%s" % oldest_query.pk
        try:
            get_backend().create_table_from_query_result(name, cursor)
        except NotImplementedError:
            # Not implemented in any backend yet!
            pass
    except Exception as ex:
        oldest_query.has_error = True
        oldest_query.error = str(ex)

    oldest_query.is_finished = True
    oldest_query.date_finished = timezone.now()
    oldest_query.save()
