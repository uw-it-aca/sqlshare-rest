from django.core.urlresolvers import reverse
from unittest2 import skipIf
from datetime import datetime
from dateutil import parser
from django.test.utils import override_settings
from django.test.client import Client
from sqlshare_rest.test.api.base import BaseAPITest
from sqlshare_rest.test import missing_url
from sqlshare_rest.dao.dataset import create_dataset_from_query, set_dataset_accounts
from sqlshare_rest.dao.dataset import set_dataset_accounts, add_public_access
from sqlshare_rest.dao.dataset import remove_public_access
from sqlshare_rest.models import Query
from sqlshare_rest.util.query_queue import process_queue
from django.utils import timezone
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
        self.assertEquals(data[0]["owner"], owner)

        creation_date = data[0]["date_created"]
        modification_date = data[0]["date_modified"]

        cd_obj = parser.parse(creation_date)
        md_obj = parser.parse(modification_date)

        now = timezone.now()

        self.assertTrue((now - cd_obj).total_seconds() < 2)
        self.assertTrue((now - md_obj).total_seconds() < 2)

        self.assertTrue((cd_obj - now).total_seconds() > -2)
        self.assertTrue((md_obj - now).total_seconds() > -2)



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


    def test_shared_url(self):
        owner = "ds_list_user3"
        other = "ds_list_shared_with1"
        self.remove_users.append(owner)
        self.remove_users.append(other)
        ds1 = create_dataset_from_query(owner, "ds_shared1", "SELECT(1)")

        auth_headers = self.get_auth_header_for_username(other)
        url = reverse("sqlshare_view_dataset_shared_list")
        response = self.client.get(url, **auth_headers)

        self.assertEquals(response.content.decode("utf-8"), "[]")

        set_dataset_accounts(ds1, [ other ])

        response = self.client.get(url, **auth_headers)

        ds_list = json.loads(response.content.decode("utf-8"))

        self.assertEquals(len(ds_list), 1)
        self.assertEquals(ds_list[0]["owner"], "ds_list_user3")
        self.assertEquals(ds_list[0]["name"], "ds_shared1")


    def test_all_url(self):
        owner1 = "ds_list_user4"
        owner2 = "ds_list_user5"
        owner3 = "ds_list_user6"
        self.remove_users.append(owner1)
        self.remove_users.append(owner2)
        self.remove_users.append(owner3)

        # Purge queries, so we can build samples for the 3 below
        Query.objects.all().delete()
        auth_headers = self.get_auth_header_for_username(owner1)
        url = reverse("sqlshare_view_dataset_all_list")

        ds1 = create_dataset_from_query(owner1, "ds_owned", "SELECT(1)")
        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))

        def build_lookup(data):
            lookup = {}
            for item in data:
                owner = item["owner"]
                name = item["name"]
                if not owner in lookup:
                    lookup[owner] = {}
                lookup[owner][name] = True
            return lookup

        lookup = build_lookup(data)
        self.assertTrue(lookup["ds_list_user4"]["ds_owned"])

        ds2 = create_dataset_from_query(owner2, "ds_shared", "SELECT(1)")
        set_dataset_accounts(ds2, [ owner1 ])

        response = self.client.get(url, **auth_headers)

        data = json.loads(response.content.decode("utf-8"))
        lookup = build_lookup(data)
        self.assertTrue(lookup["ds_list_user4"]["ds_owned"])
        self.assertTrue(lookup["ds_list_user5"]["ds_shared"])

        ds3 = create_dataset_from_query(owner3, "ds_public", "SELECT(1)")
        # Make dataset public??
        add_public_access(ds3)

        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        lookup = build_lookup(data)
        self.assertTrue(lookup["ds_list_user4"]["ds_owned"])
        self.assertTrue(lookup["ds_list_user5"]["ds_shared"])
        self.assertTrue(lookup["ds_list_user6"]["ds_public"])

        # What happens with the sample data queries?
        process_queue()
        process_queue()
        process_queue()
        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        lookup = build_lookup(data)
        self.assertTrue(lookup["ds_list_user4"]["ds_owned"])
        self.assertTrue(lookup["ds_list_user5"]["ds_shared"])
        self.assertTrue(lookup["ds_list_user6"]["ds_public"])

