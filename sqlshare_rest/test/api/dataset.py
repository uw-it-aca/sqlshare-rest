from django.test import TestCase
from unittest2 import skipIf
import base64
import json
from sqlshare_rest.test import missing_url
from django.test.utils import override_settings
from django.test.client import Client
from django.core.urlresolvers import reverse
from oauth2_provider.models import get_application_model
from oauth2_provider.settings import oauth2_settings
from oauth2_provider.compat import get_user_model, urlencode, parse_qs, urlparse

@skipIf(missing_url("sqlshare_view_dataset_list"), "SQLShare REST URLs not configured")
@override_settings(MIDDLEWARE_CLASSES = (
                                'django.contrib.sessions.middleware.SessionMiddleware',
                                'django.middleware.common.CommonMiddleware',
                                'django.contrib.auth.middleware.AuthenticationMiddleware',
                                'django.contrib.auth.middleware.RemoteUserMiddleware',
                                'django.contrib.messages.middleware.MessageMiddleware',
                                'django.middleware.clickjacking.XFrameOptionsMiddleware',
                                ),
                   AUTHENTICATION_BACKENDS = ('django.contrib.auth.backends.ModelBackend',)
                   )

class DatsetAPITest(TestCase):
    def get_basic_auth_header(self, user, password):
        """
        Return a dict containg the correct headers to set to make HTTP Basic Auth request
        """
        user_pass = '{0}:{1}'.format(user, password)
        auth_string = base64.b64encode(user_pass.encode('utf-8'))
        auth_headers = {
            'HTTP_AUTHORIZATION': 'Basic ' + auth_string.decode("utf-8"),
        }
        return auth_headers

    def setUp(self):
        self.client = Client()

    def test_unauthenticated(self):
        url = reverse("sqlshare_view_dataset_list")
        response = self.client.get(url)
        self.assertEquals(response.status_code, 403)

    def test_methods(self):
        oauth2_settings.ALLOWED_REDIRECT_URI_SCHEMES = ['http', 'custom-scheme', 'http://ok']
        Application = get_application_model()
        UserModel = get_user_model()
        self.dev_user = UserModel.objects.create_user("dev_user", "dev@user.com", "123456")
        app = Application(
            name="Test Application",
            redirect_uris="http://ok",
            user=self.dev_user,
            client_type=Application.CLIENT_CONFIDENTIAL,
            authorization_grant_type=Application.GRANT_AUTHORIZATION_CODE,
        )
        app.save()

        self.client.login(username="dev_user", password="123456")
        authcode_data = {
            'client_id': app.client_id,
            'state': 'random_state_string',
            'scope': 'read write',
            'redirect_uri': 'http://ok',
            'response_type': 'code',
            'allow': True,
        }
        response = self.client.post(reverse('oauth2_provider:authorize'), data=authcode_data)
        query_dict = parse_qs(urlparse(response['Location']).query)
        authorization_code = query_dict['code'].pop()

        token_request_data = {
            'grant_type': 'authorization_code',
            'code': authorization_code,
            'redirect_uri': 'http://ok'
        }

        auth_headers = self.get_basic_auth_header(app.client_id, app.client_secret)

        response = self.client.post(reverse('oauth2_provider:token'), data=token_request_data, **auth_headers)

        data = json.loads(response.content.decode("utf-8"))

        auth_headers = {
            'HTTP_AUTHORIZATION': 'Bearer ' + data["access_token"],
        }
        url = reverse("sqlshare_view_dataset_list")
        response = self.client.get(url, **auth_headers)

        self.assertEquals(response.content.decode("utf-8"), '[]')
