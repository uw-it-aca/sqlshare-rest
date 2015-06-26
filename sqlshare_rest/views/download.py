from oauth2_provider.decorators import protected_resource
from django.views.decorators.csrf import csrf_exempt
from django.core.urlresolvers import reverse
from django.http import HttpResponse
from sqlshare_rest.views import get_oauth_user, get403, get404, get400, get405
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.views.sql import response_for_query
from sqlshare_rest.models import DownloadToken
from sqlshare_rest.dao.user import get_user
import json


@csrf_exempt
def run(request, token):
    if request.META['REQUEST_METHOD'] != "GET":
        return get405()

    get_oauth_user(request)

    try:
        dt = DownloadToken().validate_token(token)
    except DownloadToken.DoesNotExist:
        return get404()

    sql = dt.sql

    backend = get_backend()
    user = dt.original_user
    return response_for_query(sql, user, download_name="query_results.csv")


@csrf_exempt
@protected_resource()
def init(request):
    if request.META['REQUEST_METHOD'] != "POST":
        return get405()

    get_oauth_user(request)

    values = json.loads(request.body.decode("utf-8"))
    sql = values["sql"]
    user = get_user(request)
    dt = DownloadToken()
    dt.store_token_for_sql(sql, user)

    url = reverse("sqlshare_view_run_download", kwargs={"token": dt.token})
    response = HttpResponse(json.dumps({'token': dt.token}))
    response["Location"] = url
    return response
