from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from oauth2_provider.decorators import protected_resource
import json
from sqlshare_rest.exceptions import InvalidAccountException
from sqlshare_rest.models import Dataset, User, DatasetSharingEmail
from sqlshare_rest.views import get_oauth_user, get400, get403, get404
from sqlshare_rest.dao.dataset import create_dataset_from_query
from sqlshare_rest.dao.dataset import get_dataset_by_owner_and_name
from sqlshare_rest.dao.dataset import set_dataset_accounts, set_dataset_emails
from sqlshare_rest.dao.dataset import add_account_to_dataset


@csrf_exempt
@protected_resource()
def permissions(request, owner, name):
    get_oauth_user(request)

    if owner != request.user.username:
        return get403()

    try:
        dataset = get_dataset_by_owner_and_name(owner, name)
    except Dataset.DoesNotExist:
        return get404()
    except User.DoesNotExist:
        return get404()
    except Exception as ex:
        raise

    if request.META['REQUEST_METHOD'] == "GET":
        return _get_dataset_permissions(request, dataset)

    elif request.META['REQUEST_METHOD'] == "PUT":
        return _set_dataset_permissions(request, dataset)


def _get_dataset_permissions(request, dataset):
    # The list() is needed for python3
    emails = DatasetSharingEmail.objects.filter(dataset=dataset)
    data = {
        "is_public": dataset.is_public,
        "is_shared": dataset.is_shared,
        "accounts": list(map(lambda x: x.json_data(),
                             dataset.shared_with.all())),
        "emails": list(map(lambda x: x.email.email, emails)),
    }

    return HttpResponse(json.dumps(data))


def _set_dataset_permissions(request, dataset):
    data = json.loads(request.body.decode("utf-8"))

    accounts = data.get("accounts", [])
    is_shared = False

    try:
        set_dataset_accounts(dataset, accounts, save_dataset=False)
        if len(accounts):
            is_shared = True
    except InvalidAccountException:
        return get400()

    emails = data.get("emails", [])
    if len(emails):
        is_shared = True

    set_dataset_emails(dataset, emails, save_dataset=False)

    dataset.is_shared = is_shared

    if "is_public" in data:
        dataset.is_public = data["is_public"]

    dataset.save()

    return HttpResponse()


@csrf_exempt
@protected_resource()
def add_token_access(request, token):
    get_oauth_user(request)
    try:
        sharing_email = DatasetSharingEmail.objects.get(access_token=token)
    except DatasetSharingEmail.DoesNotExist:
        response = HttpResponse("")
        response.status_code = 404
        return response

    dataset = sharing_email.dataset
    add_account_to_dataset(dataset, request.user.username)
    return HttpResponse(json.dumps(dataset.json_data()))
