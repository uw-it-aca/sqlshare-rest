from sqlshare_rest.views import RESTView, get_oauth_user
from sqlshare_rest.dao.dataset import get_datasets_owned_by_user
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from oauth2_provider.decorators import protected_resource
import json


@csrf_exempt
@protected_resource()
def dataset_list(request):
    get_oauth_user(request)

    datasets = get_datasets_owned_by_user(request.user)

    data = []
    for dataset in datasets:
        data.append(dataset.json_data())
    return HttpResponse(json.dumps(data))
