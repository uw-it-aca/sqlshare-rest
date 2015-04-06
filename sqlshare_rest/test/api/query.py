from django.test import TestCase
from unittest2 import skipIf
from django.db import connection
import json
import re
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

class QueryAPITest(BaseAPITest):
    def setUp(self):
        # Try to cleanup from any previous test runs...
        self.remove_users = []
        self.client = Client()

    def test_start_query(self):
        owner = "query_user1"
        self.remove_users.append(owner)

        post_url = reverse("sqlshare_view_query_list")
        auth_headers = self.get_auth_header_for_username(owner)

        data = {
            "sql": "select(1)"
        }

        response = self.client.post(post_url, data=json.dumps(data), content_type='application/json', **auth_headers)

        self.assertEquals(response.status_code, 201)

        values = json.loads(response.content.decode("utf-8"))

        self.assertEquals(values["error"], None)
        self.assertEquals(values["sql_code"], "select(1)")
        url = values["url"]

        self.assertTrue(re.match("/v3/db/query/[\d]+$", url))
