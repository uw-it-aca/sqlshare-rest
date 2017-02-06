from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render_to_response
from oauth2_provider.decorators import protected_resource
from django.views.decorators.csrf import csrf_exempt
from sqlshare_rest.models import User, LogoutToken
from sqlshare_rest.views import get_oauth_user
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.dao.user import get_user
from sqlshare_rest.logger import getLogger
from django.core.urlresolvers import reverse
from django.contrib.auth import logout as django_logout
from django.contrib.auth.decorators import login_required
import json

logger = getLogger(__name__)


@csrf_exempt
@protected_resource()
def user(request):
    get_oauth_user(request)

    user = get_user(request)

    data = {
        "username": user.username,
        "schema": user.schema,
    }

    logger.info("User logged in", request)
    return HttpResponse(json.dumps(data))


@csrf_exempt
@protected_resource()
def logout_init(request):
    get_oauth_user(request)

    user = get_user(request)
    token = LogoutToken()
    token.store_token_for_user(user)
    t = token.token

    data = json.dumps({"url": reverse('user_logout', kwargs={'token': t})})

    return HttpResponse(data)


@login_required
def logout_process(request, token):
    post_logout_url = _get_post_logout_url(request)
    try:
        token_obj = LogoutToken.objects.get(token=token)

        if request.user.username == token_obj.user.username:
            username = request.user.username
            django_logout(request)
            token_obj.delete()

            # This is a crude proxy for google accounts
            if username.find("@") > 0:
                # Logout google account
                logout_url = _get_google_logout_url(request)
                return HttpResponseRedirect(logout_url)
            else:
                # Log out uw account
                logout_url = _get_uw_logout_url(request)
                return HttpResponseRedirect(logout_url)
    except LogoutToken.DoesNotExist:
        pass

    return HttpResponseRedirect(post_logout_url)


def post_logout(request):
    return render_to_response("post_logout.html", {})


def _get_post_logout_url(request):
    protocol = "http://"
    if request.is_secure():
        protocol = "https://"

    host = request.get_host()

    url = reverse("post_logout_url")

    return protocol + host + url


def _get_uw_logout_url(request):
    """
    Logout url for the uw.  Lots of hard-coded values in here.
    Returns https://<your-host>/user_logout
    """
    host = request.get_host()

    return "https://"+host+"/user_logout"


def _get_google_logout_url(request):
    post_logout_url = _get_post_logout_url(request)
    return "https://www.google.com/accounts/Logout"
