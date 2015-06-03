from sqlshare_rest.views import get_oauth_user
from sqlshare_rest.dao.dataset import get_datasets_owned_by_user
from sqlshare_rest.dao.dataset import get_datasets_shared_with_user
from sqlshare_rest.dao.dataset import get_all_datasets_tagged_for_user
from sqlshare_rest.dao.dataset import get_all_datasets_for_user
from sqlshare_rest.dao.user import get_user
from sqlshare_rest.logger import getLogger
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from oauth2_provider.decorators import protected_resource
import json

logger = getLogger(__name__)


@csrf_exempt
@protected_resource()
def dataset_list(request):
    get_oauth_user(request)

    user = get_user(request)
    datasets = get_datasets_owned_by_user(user)

    data = []
    for dataset in datasets:
        data.append(dataset.json_data())

    logger.info("GET my dataset list", request)
    return HttpResponse(json.dumps(data))


@csrf_exempt
@protected_resource()
def dataset_shared_list(request):
    get_oauth_user(request)

    user = get_user(request)

    datasets = get_datasets_shared_with_user(user)

    data = []
    for dataset in datasets:
        data.append(dataset.json_data())
    logger.info("GET shared dataset list", request)
    return HttpResponse(json.dumps(data))


@csrf_exempt
@protected_resource()
def dataset_tagged_list(request, tag):
    get_oauth_user(request)
    user = get_user(request)

    datasets = get_all_datasets_tagged_for_user(user, tag_label=tag)

    data = []
    for dataset in datasets:
        data.append(dataset.json_data())
    logger.info("GET tagged dataset list; tag: %s" % (tag), request)
    return HttpResponse(json.dumps(data))


@csrf_exempt
@protected_resource()
def dataset_all_list(request):
    get_oauth_user(request)

    user = get_user(request)
    datasets = get_all_datasets_for_user(user)

    data = []
    for dataset in datasets:
        data.append(dataset.json_data())
    logger.info("GET all dataset list", request)
    return HttpResponse(json.dumps(data))
