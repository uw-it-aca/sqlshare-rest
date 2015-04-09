from django.conf.urls import patterns, include, url
from django.views.decorators.csrf import csrf_exempt

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

    url('v3/db/file/(?P<id>[0-9]+)/finalize',
        'file_upload.finalize',
        name="sqlshare_view_upload_finalize"),

    url('v3/db/file/(?P<id>[0-9]+)/parser',
        'file_parser.parser',
        name="sqlshare_view_file_parser"),

    url('v3/db/file/(?P<id>[0-9]+)',
        'file_upload.upload',
        name="sqlshare_view_file_upload"),

    url('v3/db/file/',
        'file_upload.initialize',
        name="sqlshare_view_file_upload_init"),
)
