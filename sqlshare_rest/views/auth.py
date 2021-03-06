from django.contrib.auth import authenticate, login
from django.core.urlresolvers import reverse
from django.contrib.auth.views import login as login_view
from sqlshare_rest.models import CredentialsModel
from django.contrib.auth.models import User
from django.shortcuts import redirect, render_to_response
from django.conf import settings
from apiclient.discovery import build
import httplib2
import six

if six.PY2:
    from urllib import quote
if six.PY3:
    from urllib.parse import quote


def wayf(request):
    # the google login flow requires a session id, so make sure the session
    # is saved early.
    request.session.modified = True
    return login_view(request)


def require_uw_login(request):
    login = request.META['REMOTE_USER']
    name = request.META.get('givenName', '')
    last_name = request.META.get('sn', '')
    email = request.META.get('mail', '')

    return _login_user(request, login, name, last_name, email)


def require_google_login(request):
    email = request.META['mail']
    name = request.META.get('givenname', '')
    last_name = request.META.get('surname', '')

    return _login_user(request, email, name, last_name, email)


def _login_user(request, login_name, name, last_name, email):
    user = authenticate(username=login_name, password=None)
    user.first_name = name
    user.last_name = last_name
    user.email = email
    user.save()

    login(request, user)

    next_url = request.GET.get('next', settings.SQLSHARE_WEB_URL)
    return redirect(next_url)


def google_return(request):
    f = FlowModel.objects.get(id=request.session.session_key)

    try:
        credential = f.flow.step2_exchange(request.REQUEST)
    except FlowExchangeError as ex:
        if ex[0] == "access_denied":
            return render_to_response("oauth2/denied.html", {})
        raise

    flow = f.flow
    if type(flow) == 'str':
        flow = f.flow.to_python()

    storage = Storage(CredentialsModel,
                      'id',
                      request.session.session_key,
                      'credential')
    storage.put(credential)

    google_login_url = reverse('sqlshare_rest.views.auth.require_google_login')
    google_login_url = "%s?next=%s" % (google_login_url,
                                       quote(request.GET['state']))

    return redirect(google_login_url)
