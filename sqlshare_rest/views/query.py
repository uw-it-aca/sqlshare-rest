from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from oauth2_provider.decorators import protected_resource
from sqlshare_rest.views import get_oauth_user, get403, get404
from sqlshare_rest.dao.query import create_query, get_recent_activity
from sqlshare_rest.dao.user import get_user
from sqlshare_rest.models import Query
from sqlshare_rest.util.query import get_sample_data_for_query
from sqlshare_rest.util.queue_triggers import trigger_query_queue_processing
from sqlshare_rest.logger import getLogger
import datetime
import json

logger = getLogger(__name__)


@csrf_exempt
@protected_resource()
def details(request, id):
    get_oauth_user(request)

    try:
        query = Query.objects.get(pk=id)
    except Query.DoesNotExist:
        return get404()

    user = get_user(request)
    if query.owner.username != user.username:
        return get403()

    if request.META['REQUEST_METHOD'] == "DELETE":
        return _delete_query(request, id, query)

    return _get_query(request, id, query)


def json_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()


def _get_query(request, id, query):
    data = query.json_data(request)
    user = get_user(request)

    sample_data, columns = get_sample_data_for_query(query,
                                                     user.username)

    data["sample_data"] = sample_data
    data["columns"] = columns

    response = HttpResponse(json.dumps(data, default=json_serializer))

    if not query.is_finished:
        response.status_code = 202
        logger.info("GET unfinished query; ID: %s" % (query.pk), request)
    else:
        logger.info("GET finished query; ID: %s" % (query.pk), request)

    return response


def _delete_query(request, id, query):
    query.terminated = True
    query.save()
    logger.info("Cancelled query; ID: %s" % (query.pk), request)
    trigger_query_queue_processing()

    return HttpResponse("")
