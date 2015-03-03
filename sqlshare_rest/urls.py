from django.conf.urls import patterns, include, url
from django.views.decorators.csrf import csrf_exempt

from sqlshare_rest.views.dataset_list import DatasetListView
from sqlshare_rest.views.dataset import DatasetView
from sqlshare_rest.views.dataset_permissions import DatasetPermissionsView
from sqlshare_rest.views.dataset_tags import DatasetTagsView

from sqlshare_rest.views.query_list import QueryListView
from sqlshare_rest.views.query import QueryView

from sqlshare_rest.views.file_upload import FileUploadView
from sqlshare_rest.views.file_parser import FileParserView

urlpatterns = patterns(
    url('v3/db/dataset/(?P<owner>[^/].*)/(?P<name>[^/].*)',
        csrf_exempt(DatasetView().run),
        name="sqlshare_view_dataset"),

    url('v3/db/dataset/(?P<owner>[^/].*)/(?P<name>[^/].*)/permissions',
        csrf_exempt(DatasetPermissionsView().run),
        name="sqlshare_view_dataset_permissions"),

    url('v3/db/dataset/(?P<owner>[^/].*)/(?P<name>[^/].*)/tags',
        csrf_exempt(DatasetTagsView().run),
        name="sqlshare_view_dataset_tags"),

    url('v3/db/dataset',
        csrf_exempt(DatasetListView().run),
        name="sqlshare_view_dataset_list"),

    url('v3/db/query/(?P<id>[0-9]+)',
        csrf_exempt(QueryView().run),
        name="sqlshare_view_query"),

    url('v3/db/query',
        csrf_exempt(QueryListView().run),
        name="sqlshare_view_query_list"),

    url('v3/db/file/(?P<id>[0-9]+)',
        csrf_exempt(FileUploadView().run),
        name="sqlshare_view_file_upload"),

    url('v3/db/file/(?P<id>[0-9]+)/parser',
        csrf_exempt(FileParserView().run),
        name="sqlshare_view_file_parser"),

    url('v3/db/file/',
        csrf_exempt(FileUploadView().run),
        name="sqlshare_view_file_upload"),
)
