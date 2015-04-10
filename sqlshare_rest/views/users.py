from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from oauth2_provider.decorators import protected_resource
import json
from sqlshare_rest.models import User


@csrf_exempt
@protected_resource()
def search(request):
    users = User.objects.filter(username__icontains=request.GET['q'])

    data = { "users": [] }

    for user in users:
        data["users"].append({
            "login": user.username,
            "name": "",
            "surname": "",
            "email": "",
        })

    return HttpResponse(json.dumps(data), content_type="application/json")
