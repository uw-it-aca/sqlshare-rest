from django.core.urlresolvers import reverse
from unittest2 import skipIf
from django.test.utils import override_settings
from django.test.client import Client
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.test.api.base import BaseAPITest
from sqlshare_rest.test import missing_url
from sqlshare_rest.dao.dataset import create_dataset_from_query
from testfixtures import LogCapture
import json

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

class UserAPITest(BaseAPITest):
    def setUp(self):
        # Try to cleanup from any previous test runs...
        self.remove_users = []
        self.client = Client()


    def test_pre_existing_user(self):
        user = "user_api_test1"
        self.remove_users.append(user)
        backend = get_backend()
        backend.get_user(user)

        user_auth_headers = self.get_auth_header_for_username(user)
        url = reverse("sqlshare_view_user")

        with LogCapture() as l:
            response = self.client.get(url, **user_auth_headers)
            self.assertEquals(response.status_code, 200)

            data = json.loads(response.content.decode("utf-8"))

            self.assertEquals(data["username"], user)
            self.assertEquals(data["schema"], user)

            self.assertTrue(self._has_log(l, user, None, 'sqlshare_rest.views.user', 'INFO', 'User logged in'))

    def test_new_user(self):
        user = "user_api_test2"
        self.remove_users.append(user)

        user_auth_headers = self.get_auth_header_for_username(user)
        url = reverse("sqlshare_view_user")

        response = self.client.get(url, **user_auth_headers)
        self.assertEquals(response.status_code, 200)

        data = json.loads(response.content.decode("utf-8"))

        self.assertEquals(data["username"], user)
        self.assertEquals(data["schema"], user)

    def test_scoped_login_name(self):
        user = "user_api_test2@identity.edu"
        self.remove_users.append(user)

        user_auth_headers = self.get_auth_header_for_username(user)
        url = reverse("sqlshare_view_user")

        response = self.client.get(url, **user_auth_headers)
        self.assertEquals(response.status_code, 200)

        data = json.loads(response.content.decode("utf-8"))

        self.assertEquals(data["username"], user)
        self.assertEquals(data["schema"], get_backend().get_db_schema(user))

