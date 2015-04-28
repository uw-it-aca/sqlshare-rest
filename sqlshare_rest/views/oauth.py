from oauth2_provider.views import AuthorizationView
from django.core.urlresolvers import reverse
from django.shortcuts import render_to_response
import re


class SSAuthorizationView(AuthorizationView):

    def post(self, *args, **kwargs):
        value = super(SSAuthorizationView, self).post(args, **kwargs)

        location = value.get("Location", "")
        if location and re.match("oob://", location):
            new_base = reverse("oauth_access_code")
            new_url = "%s?code=" % new_base
            location = re.sub("^oob://.*\?code=", new_url, location)
            value["Location"] = location
        return value


def access_code(request):
    return render_to_response("oauth2/access_code.html",
                              {"code": request.GET["code"]})
