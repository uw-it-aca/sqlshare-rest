from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from oauth2_provider.decorators import protected_resource
from sqlshare_rest.views import get_oauth_user, get403, get404
from sqlshare_rest.dao.query import create_query, get_recent_activity
import json


@csrf_exempt
@protected_resource()
def query_list(request):
    get_oauth_user(request)
    if request.META['REQUEST_METHOD'] == "GET":
        return _get_query_list(request)

    if request.META['REQUEST_METHOD'] == "POST":
        return _start_query(request)


def _get_query_list(request):
    queries = get_recent_activity(request.user.username)
    data = list(map(lambda x: x.json_data(request), queries))
    return HttpResponse(json.dumps(data))


def _start_query(request):
    data = json.loads(request.body.decode("utf-8"))
    sql = data["sql"]

    query = create_query(request.user.username, data["sql"])

    response = HttpResponse(json.dumps(query.json_data(request)))
    response["Location"] = query.get_url()
    response.status_code = 202
    return response
