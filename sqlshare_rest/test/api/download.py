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

class DownloadAPITest(BaseAPITest):
    def setUp(self):
        # Try to cleanup from any previous test runs...
        self.remove_users = []
        self.client = Client()
        owner = "query_user1"
        self.remove_users.append(owner)

        post_url = reverse("sqlshare_view_query_list")
        auth_headers = self.get_auth_header_for_username(owner)

        data = {
            "sql": "select(1)"
        }

        response = self.client.post(post_url, data=json.dumps(data), content_type='application/json', **auth_headers)

        values = json.loads(response.content.decode("utf-8"))
        url = values["url"]
        query_id = url[-1:]
        self.query_id = query_id




    def test_downlaod(self):
        owner = "query_user1"
        self.remove_users.append(owner)

        post_url = reverse("sqlshare_view_init_download", kwargs={'id': self.query_id})
        auth_headers = self.get_auth_header_for_username(owner)


        response = self.client.post(post_url, content_type='application/json', **auth_headers)
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.content)
        self.assertTrue('token' in body)

        token = body['token']

        post_url = reverse("sqlshare_view_run_download", kwargs={'id': self.query_id, 'token': token})

        response = self.client.post(post_url, content_type='application/json', **auth_headers)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.streaming)

        response_body = ""
        for line in response.streaming_content:
            response_body += line

        self.assertEqual(response_body, "\"1\"\n\"1\"\n")
