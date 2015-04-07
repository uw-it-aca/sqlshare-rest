from django.conf.urls import patterns, include, url
from django.views.decorators.csrf import csrf_exempt

from sqlshare_rest.views.file_upload import FileUploadView
from sqlshare_rest.views.file_parser import FileParserView

urlpatterns = patterns(
    'sqlshare_rest.views',
    url('v3/db/dataset/(?P<owner>[^/].*)/(?P<name>[^/].*)/permissions',
        'dataset_permissions.permissions',
        name="sqlshare_view_dataset_permissions"),

    url('v3/db/dataset/(?P<owner>[^/].*)/(?P<name>[^/].*)/tags',
        'dataset_tags.tags',
        name="sqlshare_view_dataset_tags"),

    url('v3/db/dataset/(?P<owner>[^/].*)/(?P<name>[^/].*)',
        'dataset.dataset',
        name="sqlshare_view_dataset"),

    url('v3/db/dataset', 'dataset_list.dataset_list',
        name="sqlshare_view_dataset_list"),

    url('v3/user/me', 'user.user',
        name="sqlshare_view_user"),

    url('v3/db/query/(?P<id>[0-9]+)',
        'query.details',
        name="sqlshare_view_query"),

    url('v3/db/query',
        'query_list.query_list',
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
