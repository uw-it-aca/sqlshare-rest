from django.test import TestCase
from unittest2 import skipIf
from django.db import connection
import json
import re
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.test import missing_url
from sqlshare_rest.models import Query
from sqlshare_rest.util.query_queue import process_queue
from django.test.utils import override_settings
from django.test.client import Client
from django.core.urlresolvers import reverse
from sqlshare_rest.test.api.base import BaseAPITest
from sqlshare_rest.dao.dataset import create_dataset_from_query
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
        Query.objects.all().delete()

        post_url = reverse("sqlshare_view_query_list")
        auth_headers = self.get_auth_header_for_username(owner)

        data = {
            "sql": "select(1)"
        }

        with LogCapture() as l:
            response = self.client.post(post_url, data=json.dumps(data), content_type='application/json', **auth_headers)

            self.assertEquals(response.status_code, 202)

            values = json.loads(response.content.decode("utf-8"))

            self.assertEquals(values["error"], None)
            self.assertEquals(values["sql_code"], "select(1)")
            url = values["url"]

            self.assertTrue(re.match("/v3/db/query/[\d]+$", url))

            qid = re.match("/v3/db/query/([\d]+)$", url).groups()[0]

            self.assertTrue(self._has_log(l, owner, None, 'sqlshare_rest.views.query_list', 'INFO', 'Started query; ID: %s; SQL: select(1)' % (qid)))

        with LogCapture() as l:
            response = self.client.get(url, **auth_headers)

            self.assertEquals(response.status_code, 202)
            values = json.loads(response.content.decode("utf-8"))

            self.assertTrue(self._has_log(l, owner, None, 'sqlshare_rest.views.query', 'INFO', 'GET unfinished query; ID: %s' % (qid)))

        process_queue(run_once=True)
        with LogCapture() as l:
            response = self.client.get(url, **auth_headers)

            self.assertEquals(response.status_code, 200)
            values = json.loads(response.content.decode("utf-8"))

            self.assertTrue(self._has_log(l, owner, None, 'sqlshare_rest.views.query', 'INFO', 'GET finished query; ID: %s' % (qid)))

