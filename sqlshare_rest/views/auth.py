from django.contrib.auth import authenticate, login
from django.core.urlresolvers import reverse
from django.contrib.auth.views import login as login_view
from sqlshare_rest.models import CredentialsModel, FlowModel
from django.contrib.auth.models import User
from django.shortcuts import redirect, render_to_response
from django.conf import settings
from apiclient.discovery import build
from oauth2client.django_orm import Storage
from oauth2client.client import OAuth2WebServerFlow, FlowExchangeError
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
    storage = Storage(CredentialsModel,
                      'id',
                      request.session.session_key,
                      'credential')
    credential = storage.get()
    scope = ('https://www.googleapis.com/auth/plus.login '
             'https://www.googleapis.com/auth/userinfo.email')
    if credential is None or credential.invalid is True:
        flow = OAuth2WebServerFlow(client_id=settings.GOOGLE_OAUTH_KEY,
                                   client_secret=settings.GOOGLE_OAUTH_SECRET,
                                   scope=scope,
                                   user_agent='plus-django-sample/1.0',
                                   state=request.GET['next'],
                                   redirect_uri=settings.GOOGLE_RETURN_URL)

        authorize_url = flow.step1_get_authorize_url()

        f = FlowModel(id=request.session.session_key, flow=flow)
        f.save()

        return redirect(authorize_url)

    http = httplib2.Http()
    plus = build('plus', 'v1', http=http)
    credential.authorize(http)
    name_data = plus.people().get(userId='me').execute()

    name = name_data["name"]["givenName"]
    last_name = name_data["name"]["familyName"]

    plus = build('oauth2', 'v2', http=http)
    credential.authorize(http)
    email_data = plus.userinfo().get().execute()
    email = email_data["email"]

    return _login_user(request, email, name, last_name, email)


def _login_user(request, login_name, name, last_name, email):
    user = authenticate(username=login_name, password=None)
    user.first_name = name
    user.last_name = last_name
    user.email = email
    user.save()

    login(request, user)

    return redirect(request.GET['next'])


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
