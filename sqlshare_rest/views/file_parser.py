from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from oauth2_provider.decorators import protected_resource
from sqlshare_rest.models import FileUpload
from sqlshare_rest.views import get_oauth_user, get403, get404, get400
from sqlshare_rest.parser import Parser
from sqlshare_rest.dao.user import get_user
from sqlshare_rest.logger import getLogger
from sqlshare_rest.parser import open_encoded
import chardet
import json

logger = getLogger(__name__)


@csrf_exempt
@protected_resource()
def parser(request, id):
    get_oauth_user(request)

    try:
        upload = FileUpload.objects.get(pk=id)
    except FileUpload.DoesNotExist:
        return get404()

    user = get_user(request)
    if upload.owner.username != user.username:
        return get403()

    if request.META["REQUEST_METHOD"] == "PUT":
        p = Parser()
        values = json.loads(request.body.decode("utf-8"))
        delimiter = values["parser"]["delimiter"]
        has_column_header = values["parser"]["has_column_header"]
        p.delimiter(delimiter)
        p.has_header_row(has_column_header)

        logger.info("File upload, PUT parser; ID: %s; delimiter: %s; "
                    "has_column_header: %s" % (upload.pk,
                                               delimiter,
                                               has_column_header),
                    request)
        _update_from_parser(upload, p)

    if not upload.has_parser_values:
        try:
            p = Parser()

            file_path = upload.user_file.path
            handle = open_encoded(file_path, "U")

            p.guess(handle.read())
            handle.close()

            _update_from_parser(upload, p)
        except Exception as ex:
            return get400("Error parsing file: %s" % ex)

    if request.META["REQUEST_METHOD"] == "GET":
        logger.info("File upload, GET parser; ID: %s" % (upload.pk), request)

    return HttpResponse(json.dumps(upload.parser_json_data()))


def _update_from_parser(upload, parser):
    file_path = upload.user_file.path
    handle = open_encoded(file_path, "U")
    parser.parse(handle)
    upload.has_column_header = parser.has_header_row()

    upload.delimiter = parser.delimiter()

    upload.column_list = json.dumps(parser.column_names())

    upload.user_file.seek(0)
    parser.parse(handle)

    preview = []
    for row in parser:
        preview.append(row)

    upload.sample_data = json.dumps(preview[:FileUpload.MAX_PARSER_PREVIEW])
    upload.has_parser_values = True
    upload.save()
