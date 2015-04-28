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
from sqlshare_rest.models import Query
from sqlshare_rest.util.query_queue import process_queue
import csv
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

class RunQueryAPITest(BaseAPITest):
    def setUp(self):
        self.remove_users = []
        self.client = Client()

    def test_running(self):
        user = "run_query_user1"
        self.remove_users.append(user)

        auth_headers = self.get_auth_header_for_username(user)
        url = reverse("sqlshare_view_run_query")

        response = self.client.get(url, **auth_headers)
        self.assertEquals(response.status_code, 405)
        response = self.client.post(url, { "sql": "SELECT (1" }, **auth_headers)
        self.assertEquals(response.status_code, 400)


        response = self.client.post(url, { }, **auth_headers)
        self.assertEquals(response.status_code, 400)

        response = self.client.post(url, { "sql": "SELECT (1), (2)" }, **auth_headers)
        self.assertEquals(response.status_code, 200)

        data = StringIO(response.content.decode("utf-8"))
        reader = csv.reader(data, delimiter=",")
        values = []
        for row in reader:
            values.append(row)

        self.assertEquals(len(values), 2)
        self.assertEquals(values[1][0], "1")
        self.assertEquals(values[1][1], "2")

        response = self.client.post(url, { "sql": "SELECT ('\",;\na')" }, **auth_headers)
        self.assertEquals(response.status_code, 200)

        data = StringIO(response.content.decode("utf-8"))
        reader = csv.reader(data, delimiter=",")
        values = []
        for row in reader:
            values.append(row)

        self.assertEquals(len(values), 2)
        self.assertEquals(values[1][0], '",;\na')

        self.assertEquals(response["Content-Disposition"],  'attachment; filename="query_results.csv"')
        self.assertEquals(response["Content-Type"],  'text/csv')
