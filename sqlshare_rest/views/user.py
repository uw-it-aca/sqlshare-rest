from django.http import HttpResponse
from oauth2_provider.decorators import protected_resource
from django.views.decorators.csrf import csrf_exempt
from sqlshare_rest.models import User
from sqlshare_rest.views import get_oauth_user
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.dao.user import get_user
import json


@csrf_exempt
@protected_resource()
def user(request):
    get_oauth_user(request)

    user = get_user(request)

    data = {
        "username": user.username,
        "schema": user.schema,
    }

    return HttpResponse(json.dumps(data))
