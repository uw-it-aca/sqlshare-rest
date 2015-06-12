from django.test import TestCase
from unittest2 import skipIf
from django.db import connection
import json
import re
from datetime import timedelta
from django.utils import timezone
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.test import missing_url
from django.test.utils import override_settings
from django.test.client import Client, RequestFactory
from django.core.urlresolvers import reverse
from sqlshare_rest.test.api.base import BaseAPITest
from sqlshare_rest.dao.query import create_query
from sqlshare_rest.util.query_queue import process_queue
from sqlshare_rest.models import Query
from django.contrib.auth.models import User
from testfixtures import LogCapture


@skipIf(missing_url("sqlshare_view_dataset_list"), "SQLShare REST URLs not configured")
@override_settings(MIDDLEWARE_CLASSES = (
                                'django.contrib.sessions.middleware.SessionMiddleware',
                                'django.middleware.common.CommonMiddleware',
                                'django.contrib.auth.middleware.AuthenticationMiddleware',
                                'django.contrib.auth.middleware.RemoteUserMiddleware',
                                'django.contrib.messages.middleware.MessageMiddleware',
                                'django.middleware.clickjacking.XFrameOptionsMiddleware',
                                ),
                   AUTHENTICATION_BACKENDS = ('django.contrib.auth.backends.ModelBackend',),
                   SQLSHARE_QUERY_CACHE_DB="test_ss_query_db",
                   )

class QueryListAPITest(BaseAPITest):
    def setUp(self):
        # Try to cleanup from any previous test runs...
        self.remove_users = []
        self.client = Client()

    def test_empty(self):
        owner = "query_list_user1"
        self.remove_users.append(owner)

        url = reverse("sqlshare_view_query_list")
        auth_headers = self.get_auth_header_for_username(owner)

        with LogCapture() as l:
            response = self.client.get(url, **auth_headers)

            self.assertEquals(response.status_code, 200)
            self.assertEquals(response.content.decode("utf-8"), "[]")

            self.assertTrue(self._has_log(l, owner, None, 'sqlshare_rest.views.query_list', 'INFO', 'GET query list'))


    def test_list(self):
        owner = "query_list_user2"
        self.remove_users.append(owner)

        Query.objects.all().delete()

        query1 = create_query(owner, "SELECT (1)")
        query2 = create_query(owner, "SELECT (1)")
        query3 = create_query(owner, "SELECT (1)")

        url = reverse("sqlshare_view_query_list")
        auth_headers = self.get_auth_header_for_username(owner)

        response = self.client.get(url, **auth_headers)

        self.assertEquals(response.status_code, 200)

        data = json.loads(response.content.decode("utf-8"))

        request = RequestFactory().get("/")
        request.user = User.objects.get(username=owner)

        full_data = [
            query3.json_data(request),
            query2.json_data(request),
            query1.json_data(request),
        ]

        self.assertEquals(data, full_data)

        query1.date_created = timezone.now() - timedelta(days=8)
        query1.save()

        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))

        full_data = [
            query3.json_data(request),
            query2.json_data(request),
        ]

        self.assertEquals(data, full_data)

        query2.terminated = True
        query2.save()

        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))

        full_data = [
            query3.json_data(request),
        ]

        self.assertEquals(data, full_data)


