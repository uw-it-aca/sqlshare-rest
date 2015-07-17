from sqlshare_rest.models import User, Query
from sqlshare_rest.util.db import get_backend
from django.utils import timezone
from datetime import timedelta


def create_query(username, sql, is_preview=False):
    backend = get_backend()
    user_obj = backend.get_user(username)

    if is_preview:
        sql = backend.get_preview_sql_for_query(sql)

    query = Query.objects.create(sql=sql, owner=user_obj)

    return query


def get_recent_activity(username):
    backend = get_backend()
    user_obj = backend.get_user(username)

    starting_at = timezone.now() - timedelta(days=7)
    base = Query.objects.filter(owner=user_obj,
                                is_preview_for=None,
                                terminated=False,
                                date_created__gte=starting_at)

    return base.order_by("-pk")
