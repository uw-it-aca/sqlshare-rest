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
from sqlshare_rest.util.db import is_mssql, is_mysql

import six
if six.PY2:
    from StringIO import StringIO
elif six.PY3:
    from io import StringIO


@skipIf(missing_url("sqlshare_view_dataset_list"), "SQLShare REST URLs not configured")
@override_settings(MIDDLEWARE_CLASSES = (
                                'django.contrib.sessions.middleware.SessionMiddleware',
                                'django.middleware.common.CommonMiddleware',
                                'django.contrib.auth.middleware.AuthenticationMiddleware',
                                'django.contrib.auth.middleware.RemoteUserMiddleware',
                                'django.contrib.messages.middleware.MessageMiddleware',
                                'django.middleware.clickjacking.XFrameOptionsMiddleware',
                                ),
                   SQLSHARE_QUERY_CACHE_DB="test_ss_query_db",
                   AUTHENTICATION_BACKENDS = ('django.contrib.auth.backends.ModelBackend',)
                   )
class DownloadAPITest(BaseAPITest):
    token = None
    query_id = None

    def test_download(self):
        owner = "test_dataset_download2"
        self.remove_users.append(owner)
        auth_headers = self.get_auth_header_for_username(owner)

        data = {
            "sql": "select(1)"
        }
        post_url = reverse("sqlshare_view_query_list")
        response = self.client.post(post_url, data=json.dumps(data), content_type='application/json', **auth_headers)

        values = json.loads(response.content.decode("utf-8"))
        url = values["url"]
        # grab query id from end of url
        query_id = url.split('/')[-1]

        post_url = reverse("sqlshare_view_init_download", kwargs={'id': query_id})

        response = self.client.post(post_url, content_type='application/json', **auth_headers)
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.content.decode("utf-8"))
        self.assertTrue('token' in body)

        self.token = body['token']

        post_url = reverse("sqlshare_view_run_download", kwargs={'id': query_id, 'token': self.token})
        response2 = self.client.get(post_url, content_type='application/json', **auth_headers)
        self.assertEqual(response2.status_code, 200)
        self.assertTrue(response2.streaming)

        response_body = StringIO("".join(map(lambda x: x.decode("utf-8"), response2.streaming_content))).read()

        if is_mssql():
            resp = '""\n"1"\n'
        if is_mysql():
            resp = '"1"\n"1"\n'
        else:
            resp = '"(1)"\n"1"\n'

        self.assertEqual(response_body, resp)

        # Ensure download only works once
        post_url = reverse("sqlshare_view_run_download", kwargs={'id': query_id, 'token': self.token})
        response = self.client.get(post_url, content_type='application/json', **auth_headers)
        self.assertEqual(response.status_code, 403)

    def test_bad_query(self):
        owner = "query_user1"
        self.remove_users.append(owner)

        # non-existing query
        post_url = reverse("sqlshare_view_init_download", kwargs={'id': 5948})
        auth_headers = self.get_auth_header_for_username(owner)
        response = self.client.post(post_url, content_type='application/json', **auth_headers)
        self.assertEqual(response.status_code, 404)

        # non-owned query
        owner = "test_dataset_download3"
        self.remove_users.append(owner)
        auth_headers = self.get_auth_header_for_username(owner)

        data = {
            "sql": "select(1)"
        }
        post_url = reverse("sqlshare_view_query_list")
        response = self.client.post(post_url, data=json.dumps(data), content_type='application/json', **auth_headers)

        values = json.loads(response.content.decode("utf-8"))
        url = values["url"]
        # grab query id from end of url
        query_id = url.split('/')[-1]


        owner = "not_the_owner"
        post_url = reverse("sqlshare_view_init_download", kwargs={'id': query_id})
        auth_headers = self.get_auth_header_for_username(owner)
        response = self.client.post(post_url, content_type='application/json', **auth_headers)
        self.assertEqual(response.status_code, 403)

    def test_bad_download(self):
        owner = "query_user1"
        self.remove_users.append(owner)

        # bad query id
        post_url = reverse("sqlshare_view_run_download", kwargs={'id': 123123, 'token': 'asd'})
        auth_headers = self.get_auth_header_for_username(owner)
        response = self.client.get(post_url, content_type='application/json', **auth_headers)
        self.assertEqual(response.status_code, 404)


    def test_bad_methods(self):
        owner = "query_user1"
        auth_headers = self.get_auth_header_for_username(owner)
        init_url = reverse("sqlshare_view_init_download", kwargs={'id': 5948})
        init_response = self.client.get(init_url, content_type='application/json', **auth_headers)
        self.assertEqual(init_response.status_code, 405)

        download_url = reverse("sqlshare_view_run_download", kwargs={'id': 5948, 'token' : 'asd1234'})
        download_response = self.client.post(download_url, content_type='application/json', **auth_headers)
        self.assertEqual(download_response.status_code, 405)
