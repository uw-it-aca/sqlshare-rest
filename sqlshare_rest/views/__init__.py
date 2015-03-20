from django.http import HttpResponse
from oauth2_provider.views.generic import ProtectedResourceView


class RESTView(object):
    def run(self, *args, **kwargs):
        request = args[0]
        if request.META['REQUEST_METHOD'] == "GET":
            return self.GET(*args, **kwargs)
