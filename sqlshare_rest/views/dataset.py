from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from oauth2_provider.decorators import protected_resource
import json
from sqlshare_rest.models import Dataset, User
from sqlshare_rest.views import get_oauth_user
from sqlshare_rest.dao.dataset import create_dataset_from_query
from sqlshare_rest.dao.dataset import get_dataset_by_owner_and_name


@csrf_exempt
@protected_resource()
def dataset(request, owner, name):
    get_oauth_user(request)
    if request.META['REQUEST_METHOD'] == "GET":
        return _get_dataset(request, owner, name)

    if request.META['REQUEST_METHOD'] == "PUT":
        return _put_dataset(request, owner, name)


def _get_dataset(request, owner, name):
    try:
        dataset = get_dataset_by_owner_and_name(owner, name)
    except Dataset.DoesNotExist:
        return get404()
    except User.DoesNotExist:
        return get404()
    except Exception as ex:
        raise

    if not dataset.user_has_access(request.user):
        return get403()

    return HttpResponse(json.dumps(dataset.json_data()))


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


def get404():
    response = HttpResponse("")
    response.status_code = 404
    return response


def get403():
    response = HttpResponse("")
    response.status_code = 403
    return response
