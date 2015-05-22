from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.core.files.base import ContentFile
from oauth2_provider.decorators import protected_resource
from sqlshare_rest.models import FileUpload, User
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.views import get_oauth_user, get403, get404
from sqlshare_rest.dao.user import get_user
import json


@csrf_exempt
@protected_resource()
def initialize(request):
    if request.META['REQUEST_METHOD'] == "POST":
        return _new_upload(request)


def _new_upload(request):
    get_oauth_user(request)

    user = get_user(request)
    owner = get_backend().get_user(user.username)

    new_upload = FileUpload.objects.create(owner=owner)

    content = ContentFile(request.body)

    new_upload.user_file.save("fn", content)

    response = HttpResponse(new_upload.pk)
    response.status_code = 201
    return response


@csrf_exempt
@protected_resource()
def upload(request, id):
    get_oauth_user(request)

    try:
        upload = FileUpload.objects.get(pk=id)
    except FileUpload.DoesNotExist:
        return get404()

    user = get_user(request)
    if upload.owner.username != user.username:
        return get403()

    file_path = upload.user_file.path
    handle = open(file_path, "at")

    handle.write(request.body.decode("utf-8"))

    return HttpResponse("")


@csrf_exempt
@protected_resource()
def finalize(request, id):
    get_oauth_user(request)

    try:
        upload = FileUpload.objects.get(pk=id)
    except FileUpload.DoesNotExist:
        return get404()

    user = get_user(request)
    if upload.owner.username != user.username:
        return get403()

    if request.META["REQUEST_METHOD"] == "POST":
        values = json.loads(request.body.decode("utf-8"))
        dataset_name = values["dataset_name"]
        description = values.get("description", "")
        is_public = values.get("is_public", False)
        upload.dataset_name = dataset_name
        upload.dataset_description = description
        upload.dataset_is_public = is_public
        upload.is_finalized = True
        upload.save()

    response = HttpResponse("")
    if upload.dataset_created:
        response.status_code = 201
        response["location"] = upload.dataset.get_url()
    else:
        response.status_code = 202
    return response
