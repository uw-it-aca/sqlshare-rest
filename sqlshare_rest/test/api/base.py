from django.test import TestCase
from django.core.urlresolvers import reverse
from oauth2_provider.models import get_application_model
from oauth2_provider.settings import oauth2_settings
from oauth2_provider.compat import get_user_model, urlencode, parse_qs, urlparse
import json

class BaseAPITest(TestCase):
    def get_auth_header_for_username(self, username):
        # Not actually useful until v0.7.3?
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

        return auth_headers
