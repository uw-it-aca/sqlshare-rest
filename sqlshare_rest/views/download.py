from oauth2_provider.decorators import protected_resource
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse
from sqlshare_rest.views import get_oauth_user, get403, get404, get400, get405
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.views.sql import response_for_query
from sqlshare_rest.models import Query, DownloadToken
import json


@csrf_exempt
def run(request, id, token):
    if request.META['REQUEST_METHOD'] != "GET":
        get405()

    get_oauth_user(request)
    if id is None:
        get400()

    try:
        query = Query.objects.get(pk=id)
    except Query.DoesNotExist:
        return get404()

    try:
        DownloadToken().validate_token(query, token)
    except DownloadToken.DoesNotExist:
        get403()

    sql = query.sql

    backend = get_backend()
    user = backend.get_user(request.user.username)
    return response_for_query(sql, user, download_name="query_results.csv")


@csrf_exempt
@protected_resource()
def init(request, id):
    if request.META['REQUEST_METHOD'] != "POST":
        get405()

    get_oauth_user(request)

    if id is None:
        get400()

    try:
        query = Query.objects.get(pk=id)
    except Query.DoesNotExist:
        return get404()

    if query.owner.username != request.user.username:
        return get403()

    dt = DownloadToken()
    dt.store_token_for_query(query)
    return HttpResponse(json.dumps({'token': dt.token}))