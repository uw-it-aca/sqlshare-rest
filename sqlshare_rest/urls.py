from django.conf.urls import patterns, include, url
from django.views.decorators.csrf import csrf_exempt
from oauth2_provider import views as oa_views
from sqlshare_rest.views import oauth as ss_oa_views

urlpatterns = patterns(
    'sqlshare_rest.views',
    url('v3/db/dataset/(?P<owner>[^/].*)/(?P<name>[^/].*)/permissions',
        'dataset_permissions.permissions',
        name="sqlshare_view_dataset_permissions"),

    url('v3/db/dataset/(?P<owner>[^/].*)/(?P<name>[^/].*)/tags',
        'dataset_tags.tags',
        name="sqlshare_view_dataset_tags"),

    url('v3/db/dataset/(?P<owner>[^/].*)/(?P<name>[^/].*)/result',
        'dataset.download',
        name="sqlshare_view_download_dataset"),

    url('v3/db/dataset/tagged/(?P<tag>.*)', 'dataset_list.dataset_tagged_list',
        name="sqlshare_view_dataset_tagged_list"),

    url('v3/db/dataset/(?P<owner>[^/].*)/(?P<name>[^/].*)',
        'dataset.dataset',
        name="sqlshare_view_dataset"),

    url('v3/db/dataset/shared', 'dataset_list.dataset_shared_list',
        name="sqlshare_view_dataset_shared_list"),

    url('v3/db/dataset/all', 'dataset_list.dataset_all_list',
        name="sqlshare_view_dataset_all_list"),

    url('v3/db/dataset', 'dataset_list.dataset_list',
        name="sqlshare_view_dataset_list"),

    url('v3/db/token/(?P<token>.*)',
        'dataset_permissions.add_token_access',
        name="sqlshare_token_access"),

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

    url('v3/users',
        'users.search',
        name="sqlshare_view_user_search"),

    url('v3/db/sql',
        'sql.run',
        name="sqlshare_view_run_query"),

    # This needs to be outside the block below, to keep it from
    # being namespaced.
    url(r'^access_code/$',
        'oauth.access_code',
        name="oauth_access_code"),

    url(r'^google_return', 'auth.google_return'),
    url(r'^google', 'auth.require_google_login'),
    url(r'^uw/', 'auth.require_uw_login'),

    url(r'user/', 'admin.user_override'),
)

# OAuth urls.  Doing this instead of including the oauth2_provider urls so we
# can override the authorization view to allow oob access.
LIST_TEMPLATE = "oauth_access_code/application_list.html"
DETAIL_TEMPLATE = "oauth_apps/application_detail.html"
DELETE_TEMPLATE = "oauth_apps/application_confirm_delete.html"

oauth_patterns = patterns(
    '',
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

)

urlpatterns += patterns(
    '',
    url(r'^o/', include(oauth_patterns, namespace="oauth2_provider")),
)
