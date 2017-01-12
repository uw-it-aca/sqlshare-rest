from django.conf.urls import include, url
from django.views.decorators.csrf import csrf_exempt
from oauth2_provider import views as oa_views
from oauth2client.contrib.django_util.site import urls as oauth2_urls
from sqlshare_rest.views import oauth as ss_oa_views

from sqlshare_rest.views.dataset_permissions import (permissions,
                                                     add_token_access)
from sqlshare_rest.views.dataset_tags import tags
from sqlshare_rest.views.dataset import download, snapshot, dataset
from sqlshare_rest.views.dataset_list import (dataset_tagged_list,
                                              dataset_recent_list,
                                              dataset_all_list,
                                              dataset_list,
                                              dataset_shared_list)
from sqlshare_rest.views.user import (user, logout_process, logout_init,
                                      post_logout)
from sqlshare_rest.views.download import run, init
from sqlshare_rest.views.query import details
from sqlshare_rest.views.query_list import query_list
from sqlshare_rest.views.file_upload import finalize, upload, initialize
from sqlshare_rest.views.file_parser import parser
from sqlshare_rest.views.users import search
from sqlshare_rest.views.sql import run as run_sql
from sqlshare_rest.views.oauth import access_code
from sqlshare_rest.views.auth import (google_return, require_google_login,
                                      require_uw_login)
from sqlshare_rest.views.admin import user_override

urlpatterns = [
    url('v3/db/dataset/(?P<owner>[^/].*)/(?P<name>[^/].*)/permissions',
        permissions,
        name="sqlshare_view_dataset_permissions"),

    url('v3/db/dataset/(?P<owner>[^/].*)/(?P<name>[^/].*)/tags',
        tags,
        name="sqlshare_view_dataset_tags"),

    url('v3/db/dataset/(?P<owner>[^/].*)/(?P<name>[^/].*)/result',
        download,
        name="sqlshare_view_download_dataset"),

    url('v3/db/dataset/tagged/(?P<tag>.*)', dataset_tagged_list,
        name="sqlshare_view_dataset_tagged_list"),

    url('v3/db/dataset/(?P<owner>[^/].*)/(?P<name>[^/].*)/snapshot',
        snapshot,
        name="sqlshare_dataset_snapshot"),

    url('v3/db/dataset/(?P<owner>[^/].*)/(?P<name>[^/].*)',
        dataset,
        name="sqlshare_view_dataset"),

    url('v3/db/dataset/recent', dataset_recent_list,
        name="sqlshare_view_dataset_recent_list"),

    url('v3/db/dataset/shared', dataset_shared_list,
        name="sqlshare_view_dataset_shared_list"),

    url('v3/db/dataset/all', dataset_all_list,
        name="sqlshare_view_dataset_all_list"),

    url('v3/db/dataset', dataset_list,
        name="sqlshare_view_dataset_list"),

    url('v3/db/token/(?P<token>.*)',
        add_token_access,
        name="sqlshare_token_access"),

    url('v3/user/me', user,
        name="sqlshare_view_user"),

    url(r'^v3/user/logout/(?P<token>.*)', logout_process,
        name="user_logout"),
    url(r'^v3/user/logout', logout_init),
    url(r'^logout', post_logout, name="post_logout_url"),

    url('v3/db/query/download/(?P<token>[a-z0-9]+)',
        run,
        name="sqlshare_view_run_download"),

    url('v3/db/query/download',
        init,
        name="sqlshare_view_init_download"),

    url('v3/db/query/(?P<id>[0-9]+)',
        details,
        name="sqlshare_view_query"),

    url('v3/db/query',
        query_list,
        name="sqlshare_view_query_list"),

    url('v3/db/file/(?P<id>[0-9]+)/finalize',
        finalize,
        name="sqlshare_view_upload_finalize"),

    url('v3/db/file/(?P<id>[0-9]+)/parser',
        parser,
        name="sqlshare_view_file_parser"),

    url('v3/db/file/(?P<id>[0-9]+)',
        upload,
        name="sqlshare_view_file_upload"),

    url('v3/db/file/',
        initialize,
        name="sqlshare_view_file_upload_init"),

    url('v3/users',
        search,
        name="sqlshare_view_user_search"),

    url('v3/db/sql',
        run_sql,
        name="sqlshare_view_run_query"),

    # This needs to be outside the block below, to keep it from
    # being namespaced.
    url(r'^access_code/$',
        access_code,
        name="oauth_access_code"),

    url(r'^google_return', google_return),
    url(r'^google', require_google_login, name="require_google_login"),
    url(r'^uw/', require_uw_login, name="require_uw_login"),

    url(r'user/', user_override),

]

# OAuth urls.  Doing this instead of including the oauth2_provider urls so we
# can override the authorization view to allow oob access.
LIST_TEMPLATE = "oauth_access_code/application_list.html"
DETAIL_TEMPLATE = "oauth_apps/application_detail.html"
DELETE_TEMPLATE = "oauth_apps/application_confirm_delete.html"

oauth_patterns = [
    url(r'^authorize/$',
        ss_oa_views.SSAuthorizationView.as_view(),
        name="authorize"),
    url(r'^token/$',
        oa_views.TokenView.as_view(),
        name="token"),
    url(r'^revoke_token/$',
        oa_views.RevokeTokenView.as_view(),
        name="revoke-token"),

    url(r'^applications/$',
        oa_views.ApplicationList.as_view(template_name=LIST_TEMPLATE),
        name="list"),
    url(r'^applications/register/$',
        ss_oa_views.SSApplicationRegistration.as_view(),
        name="register"),
    url(r'^applications/(?P<pk>\d+)/$',
        oa_views.ApplicationDetail.as_view(template_name=DETAIL_TEMPLATE),
        name="detail"),
    url(r'^applications/(?P<pk>\d+)/delete/$',
        oa_views.ApplicationDelete.as_view(template_name=DELETE_TEMPLATE),
        name="delete"),
    url(r'^applications/(?P<pk>\d+)/update/$',
        ss_oa_views.SSApplicationUpdate.as_view(),
        name="update"),

]


urlpatterns += [
    url(r'^o/', include(oauth_patterns, namespace="oauth2_provider")),
    url(r'^oauth2/', include(oauth2_urls))
]
