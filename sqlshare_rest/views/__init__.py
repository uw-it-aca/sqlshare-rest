from django.http import HttpResponse


class RESTView(object):
    def run(self, *args, **kwargs):
        return HttpResponse("")
