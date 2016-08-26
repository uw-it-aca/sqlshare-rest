from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.core.files.base import ContentFile
from oauth2_provider.decorators import protected_resource
from sqlshare_rest.models import FileUpload, User
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.views import get_oauth_user, get403, get404
from sqlshare_rest.dao.user import get_user
from sqlshare_rest.logger import getLogger
import json
import re

logger = getLogger(__name__)


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

    logger.info("File upload, initialized; ID: %s" % (new_upload.pk), request)
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
    handle = open(file_path, "ab")

    handle.write(request.body)
    logger.info("File upload, Append data; ID: %s" % (upload.pk), request)

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

        bad_name_response = dataset_name_invalid_check(dataset_name)
        if bad_name_response:
            return bad_name_response
        description = values.get("description", "")
        is_public = values.get("is_public", False)
        upload.dataset_name = dataset_name
        upload.dataset_description = description
        upload.dataset_is_public = is_public
        upload.is_finalized = True
        logger.info("File upload, PUT finalize; ID: %s; name: %s; "
                    "description: %s; is_public: %s" % (upload.pk,
                                                        dataset_name,
                                                        description,
                                                        is_public),
                    request)

        upload.rows_total = _get_total_upload_rows(upload)
        upload.rows_loaded = 0
        upload.save()

    if request.META["REQUEST_METHOD"] == "GET":
        logger.info("File upload, GET finalize; ID: %s" % (upload.pk), request)

    response = HttpResponse(json.dumps(upload.finalize_json_data()))

    if "rows_total" in upload.finalize_json_data():
        response = HttpResponse("Error: something something")
        response.status_code = 400
        print "response: ", response, upload.finalize_json_data()
        return response

    if upload.dataset_created:
        response.status_code = 201
        response["location"] = upload.dataset.get_url()
    elif upload.has_error:
        response = HttpResponse(upload.error)
        response.status_code = 400
    else:
        response.status_code = 202

    return response


def _get_total_upload_rows(upload):
    file_path = upload.user_file.path
    handle = open(file_path, "U")

    total_lines = 0
    for line in handle:
        total_lines += 1

    if upload.has_column_header:
        return total_lines - 1
    return total_lines


def dataset_name_invalid_check(name):
    if re.match(".*%.*", name):
        response = HttpResponse("% not allowed in dataset name")
        response.status_code = 400
        return response

    return
