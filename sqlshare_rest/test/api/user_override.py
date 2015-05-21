from django.core.urlresolvers import reverse
from unittest2 import skipIf
from django.test import RequestFactory
from django.test.utils import override_settings
from django.test.client import Client
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.test.api.base import BaseAPITest
from sqlshare_rest.test import missing_url
from sqlshare_rest.dao.dataset import create_dataset_from_query
from sqlshare_rest.models import User
from sqlshare_rest.dao.user import get_user
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

class UserOverrideAPITest(BaseAPITest):
    def setUp(self):
        # Try to cleanup from any previous test runs...
        self.remove_users = []
        self.client = Client()


    def test_api_methods(self):
        self.remove_users = []
        user = "overrider"
        self.remove_users.append(user)
        self.remove_users.append("over2")
        user_auth_headers = self.get_auth_header_for_username(user)

        backend = get_backend()
        user_obj = backend.get_user(user)
        user_obj.override_as = None
        user_obj.save()

        url = reverse("sqlshare_view_user")

        response = self.client.get(url, **user_auth_headers)
        self.assertEquals(response.status_code, 200)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["username"], user)
        self.assertEquals(data["schema"], user)


        user2 = backend.get_user("over1")
        user_obj.override_as = user2
        user_obj.save()

        response = self.client.get(url, **user_auth_headers)
        self.assertEquals(response.status_code, 200)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["username"], "over1")
        self.assertEquals(data["schema"], "over1")

