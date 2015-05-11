from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from oauth2_provider.decorators import protected_resource
import json
from datetime import datetime
from sqlshare_rest.models import Dataset, User
from sqlshare_rest.views import get_oauth_user, get403, get404
from sqlshare_rest.dao.dataset import get_dataset_by_owner_and_name
from sqlshare_rest.dao.dataset import update_tags


@csrf_exempt
@protected_resource()
def tags(request, owner, name):
    get_oauth_user(request)
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

    if request.META['REQUEST_METHOD'] == "GET":
        return _get_tags(request, dataset)

    if request.META['REQUEST_METHOD'] == "PUT":
        return _put_tags(request, dataset)


def _get_tags(request, dataset):
    return HttpResponse(json.dumps(dataset.get_tags_data()))
    pass


def _put_tags(request, dataset):
    data = json.loads(request.body.decode("utf-8"))

    is_owner = request.user.username == dataset.owner.username

    update_tagsets = []
    for user_tags in data:
        if is_owner:
            update_tagsets.append(user_tags)
        else:
            if request.user.username == user_tags["name"]:
                update_tagsets.append(user_tags)

    update_tags(dataset, update_tagsets, request.user)

    return _get_tags(request, dataset)
