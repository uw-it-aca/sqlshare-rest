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
from sqlshare_rest.models import Query, Dataset
from sqlshare_rest.util.query_queue import process_queue
from sqlshare_rest.util.snapshot_queue import process_snapshot_queue
from testfixtures import LogCapture
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

class SnapshotAPITest(BaseAPITest):
    def setUp(self):
        super(SnapshotAPITest, self).setUp()
        # Try to cleanup from any previous test runs...
        self.remove_users = []
        self.client = Client()

    def test_snapshot(self):
        owner = "snapshot_user1"
        dataset_name = "snap_source1"

        self.remove_users.append(owner)

        backend = get_backend()
        backend.get_user(owner)

        Dataset.objects.all().delete()
        ds1 = create_dataset_from_query(owner, dataset_name, "SELECT(1)")

        url = reverse("sqlshare_dataset_snapshot", kwargs={ 'owner': owner,
                                                            'name': dataset_name})

        owner_auth_headers = self.get_auth_header_for_username(owner)

        self.assertEquals(len(Dataset.objects.all()), 1)
        Query.objects.all().delete()
        self.assertEquals(len(Query.objects.all()), 0)

        new_data = {
            "name": "snap_destination1",
            "description": "Snapshot created in test",
            "is_public": True,
        }

        with LogCapture() as l:
            response = self.client.post(url, data=json.dumps(new_data), content_type="application/json", **owner_auth_headers)
            self.assertEquals(response.status_code, 201)
            self.assertEquals(response.content.decode("utf-8"), "")
            self.assertEquals(response["Location"], "/v3/db/dataset/snapshot_user1/snap_destination1")
            self.assertTrue(self._has_log(l, owner, None, 'sqlshare_rest.views.dataset', 'INFO', 'POST dataset snapshot; owner: snapshot_user1; name: snap_source1; destination_name: snap_destination1; is_public: True'))


        self.assertEquals(len(Dataset.objects.all()), 2)
        self.assertEquals(len(Query.objects.all()), 0)

        process_snapshot_queue(verbose=True)
        self.assertEquals(len(Query.objects.all()), 1)
