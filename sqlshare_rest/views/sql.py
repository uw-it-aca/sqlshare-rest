from django.http import HttpResponse
from oauth2_provider.decorators import protected_resource
from django.views.decorators.csrf import csrf_exempt
from sqlshare_rest.views import get_oauth_user
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.util.query import frontend_description_from_cursor
import json
import re


@csrf_exempt
@protected_resource()
def run(request):
    if request.META['REQUEST_METHOD'] != "POST":
        response = HttpResponse()
        response.status_code = 405
        return response

    get_oauth_user(request)

    backend = get_backend()
    user = backend.get_user(request.user.username)

    sql = request.POST.get("sql", "")
    try:
        cursor = backend.run_query(sql, user, return_cursor=True)
        disposition = 'attachment; filename="query_results.csv"'
        response = HttpResponse(stream_query(cursor), content_type='text/csv')
        response['Content-Disposition'] = disposition
        return response
    except Exception as ex:
        response = HttpResponse(str(ex))
        response.status_code = 400
        return response


def stream_query(cursor):
    columns = frontend_description_from_cursor(cursor)

    names = ",".join(list(map(lambda x: csv_encode(x["name"]), columns)))

    yield names
    yield "\n"

    row = cursor.fetchone()
    while row:
        yield ",".join(list(map(lambda x: csv_encode("%s" % x), row)))
        yield "\n"
        row = cursor.fetchone()


def csv_encode(value):
    value = re.sub('"', '""', value)

    return '"%s"' % value
