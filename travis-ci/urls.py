from django.conf.urls import patterns, include, url
from django.contrib import admin

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'project.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),
    url(r'^', include('sqlshare_rest.urls')),
    url(r'^o/', include('oauth2_provider.urls', namespace='oauth2_provider')),
)
