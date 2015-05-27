from oauth2_provider.decorators import protected_resource
from django.views.decorators.csrf import csrf_exempt
from sqlshare_rest.views import get_oauth_user, get403, get404, get400, get405
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.views.sql import response_for_query
from sqlshare_rest.models import Query


@csrf_exempt
# @protected_resource()
def run(request, id):
    if request.META['REQUEST_METHOD'] != "GET":
        get405()

    get_oauth_user(request)
    if id is None:
        get400()

    try:
        query = Query.objects.get(pk=id)
    except Query.DoesNotExist:
        return get404()

    # if query.owner.username != request.user.username:
    #     return get403()
    sql = query.sql

    backend = get_backend()
    user = backend.get_user(request.user.username)
    return response_for_query(sql, user, download_name="query_results.csv")
