from django.core.urlresolvers import reverse
from unittest2 import skipIf
from django.test.utils import override_settings
from django.test.client import Client
from sqlshare_rest.test.api.base import BaseAPITest
from sqlshare_rest.test import missing_url
from sqlshare_rest.dao.dataset import create_dataset_from_query
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

class DatsetListAPITest(BaseAPITest):
    def setUp(self):
        # Try to cleanup from any previous test runs...
        self.remove_users = []
        self.client = Client()


    def test_my_list(self):
        owner = "ds_list_user1"
        self.remove_users.append(owner)
        ds1 = create_dataset_from_query(owner, "ds1", "SELECT(1)")
        ds2 = create_dataset_from_query(owner, "ds2", "SELECT(2)")
        ds3 = create_dataset_from_query(owner, "ds3", "SELECT(3)")
        ds4 = create_dataset_from_query(owner, "ds4", "SELECT(4)")

        auth_headers = self.get_auth_header_for_username(owner)
        url = reverse("sqlshare_view_dataset_list")
        response = self.client.get(url, **auth_headers)

        data = json.loads(response.content.decode("utf-8"))

        self.assertEquals(len(data), 4)
        self.assertEquals(data[0]["sql_code"], "SELECT(1)")
        self.assertEquals(data[0]["is_public"], False)
        self.assertEquals(data[0]["name"], "ds1")

        self.assertEquals(data[1]["sql_code"], "SELECT(2)")
        self.assertEquals(data[2]["sql_code"], "SELECT(3)")
        self.assertEquals(data[3]["sql_code"], "SELECT(4)")

    def test_new_user(self):
        owner = "ds_list_user2"
        self.remove_users.append(owner)
        auth_headers = self.get_auth_header_for_username(owner)
        url = reverse("sqlshare_view_dataset_list")
        response = self.client.get(url, **auth_headers)

        self.assertEquals(response.content.decode("utf-8"), "[]")
