from django.test import TestCase
from unittest2 import skipIf
from django.db import connection
import json
from datetime import datetime
from dateutil import parser
from django.utils import timezone
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.test import missing_url
from django.test.utils import override_settings
from django.test.client import Client
from django.core.urlresolvers import reverse
from sqlshare_rest.test.api.base import BaseAPITest
from sqlshare_rest.dao.dataset import create_dataset_from_query

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

class UserSearchTest(BaseAPITest):
    def setUp(self):
        super(UserSearchTest, self).setUp()
        self.remove_users = []
        self.client = Client()

    def test_basic(self):
        user = "test_searching_for_meeee"
        self.remove_users.append(user)
        self.remove_users.append("test_searching_for_giraffe")
        user1 = get_backend().get_user(user)
        user2 = get_backend().get_user("test_searching_for_giraffe")

        auth_headers = self.get_auth_header_for_username(user)

        url = reverse("sqlshare_view_user_search")
        response = self.client.get(url, { "q": "giraffe" }, **auth_headers)
        self.assertEquals(response.status_code, 200)

        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["users"], [{ "login": "test_searching_for_giraffe", "name": "", "email": "", "surname": "" }])

