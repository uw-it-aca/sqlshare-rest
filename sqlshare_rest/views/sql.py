from django.http import HttpResponse, StreamingHttpResponse
from oauth2_provider.decorators import protected_resource
from django.views.decorators.csrf import csrf_exempt
from sqlshare_rest.views import get_oauth_user
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.util.query import frontend_description_from_cursor
import json
import re


@csrf_exempt
# @protected_resource()
def run(request):
    if request.META['REQUEST_METHOD'] != "POST":
        response = HttpResponse()
        response.status_code = 405
        return response

    get_oauth_user(request)
    sql = request.POST.get("sql", "")

    backend = get_backend()
    user = backend.get_user(request.user.username)
    return response_for_query(sql, user, download_name="query_results.csv")


def response_for_query(sql, user, download_name):
    try:
        backend = get_backend()
        if sql == "":
            raise Exception("Missing sql statement")
        cursor = backend.run_query(sql, user, return_cursor=True)
        disposition = 'attachment; filename="%s"' % download_name
        response = StreamingHttpResponse(stream_query(cursor, user),
                                         content_type='text/csv')
        response['Content-Disposition'] = disposition
        return response
    except Exception as ex:
        response = HttpResponse(str(ex))
        response.status_code = 400
        return response


def stream_query(cursor, user):
    columns = frontend_description_from_cursor(cursor)

    names = ",".join(list(map(lambda x: csv_encode(x["name"]), columns)))

    yield names
    yield "\n"

    row = cursor.fetchone()
    while row:
        yield ",".join(list(map(lambda x: csv_encode("%s" % x), row)))
        yield "\n"
        row = cursor.fetchone()

    get_backend().close_user_connection(user)


def csv_encode(value):
    value = re.sub('"', '""', value)

    return '"%s"' % value
