from django.http import HttpResponse
from oauth2_provider.views.generic import ProtectedResourceView
from oauth2_provider.oauth2_validators import OAuth2Validator


def get_oauth_user(request):
    if request.META.get('HTTP_AUTHORIZATION', '').startswith('Bearer'):
        token = request.META.get('HTTP_AUTHORIZATION', '')[7:]
        if not OAuth2Validator().validate_bearer_token(token=token,
                                                       scopes=[],
                                                       request=request):
            raise Exception("Invalid token - no user")

        if not request.user:
            raise Exception("Invalid token - no user")

        if not request.client:
            raise Exception("Invalid token - no client app")


class RESTView(object):
    def run(self, *args, **kwargs):
        request = args[0]
        if request.META['REQUEST_METHOD'] == "GET":
            return self.GET(*args, **kwargs)


def get404():
    response = HttpResponse("")
    response.status_code = 404
    return response


def get403():
    response = HttpResponse("")
    response.status_code = 403
    return response


def get400():
    response = HttpResponse("")
    response.status_code = 400
    return response
