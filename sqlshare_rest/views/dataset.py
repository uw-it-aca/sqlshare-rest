from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from oauth2_provider.decorators import protected_resource
import json
from datetime import datetime
from django.utils import timezone
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.models import Dataset, User, Query, RecentDatasetView
from sqlshare_rest.views import get_oauth_user, get403, get404, get405
from sqlshare_rest.views.sql import response_for_query
from sqlshare_rest.dao.user import get_user
from sqlshare_rest.dao.dataset import create_dataset_from_query
from sqlshare_rest.dao.dataset import create_dataset_from_snapshot
from sqlshare_rest.dao.dataset import create_preview_for_dataset
from sqlshare_rest.dao.dataset import get_dataset_by_owner_and_name
from sqlshare_rest.dao.dataset import update_dataset_sql
from sqlshare_rest.util.query import get_sample_data_for_query
from sqlshare_rest.logger import getLogger

logger = getLogger(__name__)


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

    user = get_user(request)
    if not dataset.user_has_read_access(user):
        return get403()

    backend = get_backend()
    sql = backend.get_download_sql_for_dataset(dataset)

    download_name = "%s.csv" % name
    return response_for_query(sql, user, download_name)


@csrf_exempt
@protected_resource()
def snapshot(request, owner, name):
    get_oauth_user(request)
    if request.META['REQUEST_METHOD'] != "POST":
        return get405()

    try:
        dataset = get_dataset_by_owner_and_name(owner, name)
    except Dataset.DoesNotExist:
        return get404()
    except User.DoesNotExist:
        return get404()
    except Exception as ex:
        raise

    user = get_user(request)
    if not dataset.user_has_read_access(user):
        return get403()

    values = json.loads(request.body.decode("utf-8"))
    new_name = values["name"]
    description = values["description"]
    is_public = getattr(values, "is_public", True)
    logger.info("POST dataset snapshot; owner: %s; name: %s; "
                "destination_name: %s; is_public: %s" % (owner,
                                                         name,
                                                         new_name,
                                                         is_public),
                request)

    new_dataset = create_dataset_from_snapshot(user, new_name, dataset)

    response = HttpResponse("")
    response["location"] = new_dataset.get_url()
    response.status_code = 201
    return response


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

    user = get_user(request)
    if not dataset.user_has_read_access(user):
        return get403()

    if dataset.popularity:
        dataset.popularity = dataset.popularity + 1
    else:
        dataset.popularity = 1
    dataset.last_viewed = timezone.now()
    dataset.save()

    get_or_create = RecentDatasetView.objects.get_or_create
    recent_view, created = get_or_create(dataset=dataset, user=user)
    recent_view.timestamp = timezone.now()
    recent_view.save()

    data = dataset.json_data()

    if dataset.preview_is_finished:
        username = user.username
        query = Query.objects.get(is_preview_for=dataset)

        sample_data, columns = get_sample_data_for_query(query,
                                                         username)

        data["sample_data"] = sample_data
        data["columns"] = columns

    logger.info("GET dataset; owner: %s; name: %s" % (owner, name), request)
    data["qualified_name"] = get_backend().get_qualified_name(dataset)
    return HttpResponse(json.dumps(data))


def _put_dataset(request, owner, name):
    user = get_user(request)
    username = user.username
    if username != owner:
        raise Exception("Owner doesn't match user: %s, %s" % (owner, username))

    data = json.loads(request.body.decode("utf-8"))
    try:
        dataset = create_dataset_from_query(username, name, data["sql_code"])

        description = data.get("description", "")
        is_public = data.get("is_public", False)
        dataset.description = description
        dataset.is_public = is_public

        dataset.save()

        response = HttpResponse(json.dumps(dataset.json_data()))
        response.status_code = 201

        logger.info("PUT dataset; owner: %s; name: %s" % (owner, name),
                    request)

        return response
    except Exception as ex:
        response = HttpResponse("Error saving dataset: %s" % ex)
        response.status_code = 400
        return response


def _patch_dataset(request, owner, name):
    user = get_user(request)
    username = user.username
    if username != owner:
        raise Exception("Owner doesn't match user: %s, %s" % (owner, username))

    dataset = get_dataset_by_owner_and_name(owner, name)

    data = json.loads(request.body.decode("utf-8"))

    updated = False
    if "description" in data:
        dataset.description = data["description"]
        logger.info("PATCH dataset description; owner: %s; name: %s; "
                    "description: %s" % (owner, name, data["description"]),
                    request)
        updated = True

    if "sql_code" in data:
        dataset.sql = data["sql_code"]
        updated = True
        logger.info("PATCH dataset sql_code; owner: %s; name: %s; "
                    "sql_code: %s" % (owner, name, dataset.sql), request)
        try:
            update_dataset_sql(owner, dataset, data["sql_code"])
            updated = True
        except Exception as ex:
            r = HttpResponse("Error updating sql: %s" % (str(ex)))
            r.status_code = 400
            return r

    if "is_public" in data:
        dataset.is_public = data["is_public"]
        logger.info("PATCH dataset is_public; owner: %s; name: %s; "
                    "is_public: %s" % (owner, name, dataset.is_public),
                    request)

        if dataset.is_public:
            get_backend().add_public_access(dataset, user)
        else:
            get_backend().remove_public_access(dataset, user)
        updated = True

    dataset.save()

    return HttpResponse(json.dumps(dataset.json_data()))


def _delete_dataset(request, owner, name):
    user = get_user(request)
    username = user.username
    if username != owner:
        raise Exception("Owner doesn't match user: %s, %s" % (owner, username))

    response = HttpResponse("")
    try:
        dataset = get_dataset_by_owner_and_name(owner, name)
        Query.objects.filter(is_preview_for=dataset).delete()
    except Dataset.DoesNotExist:
        response.status_code = 404
        return response

    logger.info("DELETE dataset; owner: %s; name: %s" % (owner, name), request)
    dataset.delete()
    return response
