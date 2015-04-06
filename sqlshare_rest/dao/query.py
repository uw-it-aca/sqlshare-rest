from sqlshare_rest.models import User, Query
from sqlshare_rest.util.db import get_backend


def create_query(username, sql):
    backend = get_backend()
    user_obj = backend.get_user(username)

    query = Query.objects.create(sql=sql, owner=user_obj)
    return query
