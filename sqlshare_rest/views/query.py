from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from oauth2_provider.decorators import protected_resource
from sqlshare_rest.views import get_oauth_user, get403, get404
from sqlshare_rest.dao.query import create_query, get_recent_activity
from sqlshare_rest.models import Query
import json


@csrf_exempt
@protected_resource()
def details(request, id):
    get_oauth_user(request)

    try:
        query = Query.objects.get(pk=id)
    except Query.DoesNotExist:
        return get404()

    if query.owner.username != request.user.username:
        return get403()

    return HttpResponse(json.dumps(query.json_data()))
