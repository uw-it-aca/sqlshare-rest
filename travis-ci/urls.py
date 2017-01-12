from django.conf.urls import include, url
from django.contrib.auth import views as auth_views


from django.contrib import admin
admin.autodiscover()

urlpatterns = [
    # Examples:
    # url(r'^$', 'project.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

    url(r'^', include('sqlshare_rest.urls')),
    url(r'^accounts/login/$', auth_views.login),
]
