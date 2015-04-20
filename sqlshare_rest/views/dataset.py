from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from oauth2_provider.decorators import protected_resource
import json
from datetime import datetime
from django.utils import timezone
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.models import Dataset, User, Query
from sqlshare_rest.views import get_oauth_user, get403, get404
from sqlshare_rest.views.sql import response_for_query
from sqlshare_rest.dao.dataset import create_dataset_from_query
from sqlshare_rest.dao.dataset import get_dataset_by_owner_and_name
from sqlshare_rest.util.query import get_sample_data_for_query


@csrf_exempt
@protected_resource()
def download(request, owner, name):
    get_oauth_user(request)

    if request.META['REQUEST_METHOD'] != "POST":
        response = HttpResponse("")
        response.status_code = 405
        return response

    try:
        dataset = get_dataset_by_owner_and_name(owner, name)
    except Dataset.DoesNotExist:
        return get404()
    except User.DoesNotExist:
        return get404()
    except Exception as ex:
        raise

    if not dataset.user_has_read_access(request.user):
        return get403()

    backend = get_backend()
    user = backend.get_user(request.user.username)
    sql = backend.get_download_sql_for_dataset(dataset)

    download_name = "%s.csv" % name
    return response_for_query(sql, user, download_name)


@csrf_exempt
@protected_resource()
def dataset(request, owner, name):
    get_oauth_user(request)
    if request.META['REQUEST_METHOD'] == "GET":
        return _get_dataset(request, owner, name)

    if request.META['REQUEST_METHOD'] == "PUT":
        return _put_dataset(request, owner, name)

    if request.META['REQUEST_METHOD'] == "PATCH":
        return _patch_dataset(request, owner, name)

    if request.META['REQUEST_METHOD'] == "DELETE":
        return _delete_dataset(request, owner, name)


def _get_dataset(request, owner, name):
    try:
        dataset = get_dataset_by_owner_and_name(owner, name)
    except Dataset.DoesNotExist:
        return get404()
    except User.DoesNotExist:
        return get404()
    except Exception as ex:
        raise

    if not dataset.user_has_read_access(request.user):
        return get403()

    if dataset.popularity:
        dataset.popularity = dataset.popularity + 1
    else:
        dataset.popularity = 1
    dataset.last_viewed = timezone.now()
    dataset.save()

    data = dataset.json_data()

    if dataset.preview_is_finished:
        username = request.user.username
        query = Query.objects.get(is_preview_for=dataset)
        sample_data, columns = get_sample_data_for_query(query,
                                                         username)

        data["sample_data"] = sample_data
        data["columns"] = columns

    data["qualified_name"] = get_backend().get_qualified_name(dataset)
    return HttpResponse(json.dumps(data))


def _put_dataset(request, owner, name):
    username = request.user.username
    if username != owner:
        raise Exception("Owner doesn't match user: %s, %s" % (owner, username))

    data = json.loads(request.body.decode("utf-8"))
    dataset = create_dataset_from_query(username, name, data["sql_code"])

    description = data.get("description", "")
    is_public = data.get("is_public", False)
    dataset.description = description
    dataset.is_public = is_public

    dataset.save()

    response = HttpResponse(json.dumps(dataset.json_data()))
    response.status_code = 201

    return response


def _patch_dataset(request, owner, name):
    username = request.user.username
    if username != owner:
        raise Exception("Owner doesn't match user: %s, %s" % (owner, username))

    dataset = get_dataset_by_owner_and_name(owner, name)

    data = json.loads(request.body.decode("utf-8"))

    updated = False
    if "description" in data:
        dataset.description = data["description"]
        updated = True

    dataset.save()

    return HttpResponse(json.dumps(dataset.json_data()))


def _delete_dataset(request, owner, name):
    username = request.user.username
    if username != owner:
        raise Exception("Owner doesn't match user: %s, %s" % (owner, username))

    response = HttpResponse("")
    try:
        dataset = get_dataset_by_owner_and_name(owner, name)
        Query.objects.filter(is_preview_for=dataset).delete()
    except Dataset.DoesNotExist:
        response.status_code = 404
        return response

    dataset.delete()
    return response
