from sqlshare_rest.util.db import get_backend
from sqlshare_rest.models import Query
from django.utils import timezone


def process_queue():
    filtered = Query.objects.filter(is_finished=False)

    try:
        oldest_query = filtered.order_by('id')[:1].get()
    except Query.DoesNotExist:
        return

    user = oldest_query.owner
    backend = get_backend()
    try:
        cursor = backend.run_query(oldest_query.sql,
                                   user,
                                   return_cursor=True)

        name = "query_%s" % oldest_query.pk
        try:
            backend.create_table_from_query_result(name, cursor)
            backend.add_read_access_to_query(oldest_query.pk, user)
        except:
            raise
    except Exception as ex:
        oldest_query.has_error = True
        oldest_query.error = str(ex)
    finally:
        backend.close_user_connection(user)

    oldest_query.is_finished = True
    oldest_query.date_finished = timezone.now()
    oldest_query.save()
