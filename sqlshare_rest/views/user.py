from django.http import HttpResponse
from oauth2_provider.decorators import protected_resource
from django.views.decorators.csrf import csrf_exempt
from sqlshare_rest.models import User
from sqlshare_rest.views import get_oauth_user
from sqlshare_rest.util.db import get_backend
import json


@csrf_exempt
@protected_resource()
def user(request):
    get_oauth_user(request)

    user = get_backend().get_user(request.user.username)

    data = {
        "username": user.username,
        "schema": user.schema,
    }

    return HttpResponse(json.dumps(data))
