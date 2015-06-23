from django.core.urlresolvers import reverse
from django.db import connection
from unittest2 import skipIf
from datetime import datetime
from dateutil import parser
from django.test.utils import override_settings
from django.test.client import Client
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.test.api.base import BaseAPITest
from sqlshare_rest.test import missing_url
from sqlshare_rest.dao.dataset import create_dataset_from_query, set_dataset_accounts
from sqlshare_rest.dao.dataset import set_dataset_accounts, add_public_access
from sqlshare_rest.dao.dataset import remove_public_access
from sqlshare_rest.models import Query
from sqlshare_rest.util.query_queue import process_queue
from django.utils import timezone
import json
from testfixtures import LogCapture
from time import sleep


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
        ds1 = create_dataset_from_query(owner, "dsa1", "SELECT(1)")
        ds2 = create_dataset_from_query(owner, "dsa2", "SELECT(2)")
        ds3 = create_dataset_from_query(owner, "dsa3", "SELECT(3)")
        ds4 = create_dataset_from_query(owner, "dsa4", "SELECT(4)")

        auth_headers = self.get_auth_header_for_username(owner)
        url = reverse("sqlshare_view_dataset_list")

        with LogCapture() as l:
            response = self.client.get(url, **auth_headers)

            data = json.loads(response.content.decode("utf-8"))

            self.assertEquals(len(data), 4)
            self.assertEquals(data[0]["sql_code"], "SELECT(1)")
            self.assertEquals(data[0]["is_public"], False)
            self.assertEquals(data[0]["name"], "dsa1")
            self.assertEquals(data[0]["owner"], owner)

            creation_date = data[0]["date_created"]
            modification_date = data[0]["date_modified"]

            cd_obj = parser.parse(creation_date)
            md_obj = parser.parse(modification_date)

            now = timezone.now()

            limit = get_backend().get_testing_time_delta_limit()


            self.assertTrue((now - cd_obj).total_seconds() < limit)
            self.assertTrue((now - md_obj).total_seconds() < limit)

            self.assertTrue((cd_obj - now).total_seconds() > -1 * limit)
            self.assertTrue((md_obj - now).total_seconds() > -1 * limit)



            self.assertEquals(data[1]["sql_code"], "SELECT(2)")
            self.assertEquals(data[2]["sql_code"], "SELECT(3)")
            self.assertEquals(data[3]["sql_code"], "SELECT(4)")

            self.assertTrue(self._has_log(l, "ds_list_user1", None, 'sqlshare_rest.views.dataset_list', 'INFO', 'GET my dataset list'))


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

        with LogCapture() as l:
            response = self.client.get(url, **auth_headers)

            self.assertEquals(response.content.decode("utf-8"), "[]")

            set_dataset_accounts(ds1, [ other ])

            response = self.client.get(url, **auth_headers)

            ds_list = json.loads(response.content.decode("utf-8"))

            self.assertEquals(len(ds_list), 1)
            self.assertEquals(ds_list[0]["owner"], "ds_list_user3")
            self.assertEquals(ds_list[0]["name"], "ds_shared1")

            self.assertTrue(self._has_log(l, "ds_list_shared_with1", None, 'sqlshare_rest.views.dataset_list', 'INFO', 'GET shared dataset list'))


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

        with LogCapture() as l:
            response = self.client.get(url, **auth_headers)
            data = json.loads(response.content.decode("utf-8"))
            lookup = build_lookup(data)
            self.assertTrue(lookup["ds_list_user4"]["ds_owned"])
            self.assertTrue(lookup["ds_list_user5"]["ds_shared"])
            self.assertTrue(lookup["ds_list_user6"]["ds_public"])

            # What happens with the sample data queries?

            remove_id1 = Query.objects.all()[0].pk
            remove_id2 = Query.objects.all()[1].pk
            remove_id3 = Query.objects.all()[2].pk
            process_queue()
            process_queue()
            process_queue()
            response = self.client.get(url, **auth_headers)
            data = json.loads(response.content.decode("utf-8"))
            lookup = build_lookup(data)
            self.assertTrue(lookup["ds_list_user4"]["ds_owned"])
            self.assertTrue(lookup["ds_list_user5"]["ds_shared"])
            self.assertTrue(lookup["ds_list_user6"]["ds_public"])

            get_backend().remove_table_for_query_by_name("query_%s" % remove_id1)
            get_backend().remove_table_for_query_by_name("query_%s" % remove_id2)
            get_backend().remove_table_for_query_by_name("query_%s" % remove_id3)

            self.assertTrue(self._has_log(l, "ds_list_user4", None, 'sqlshare_rest.views.dataset_list', 'INFO', 'GET all dataset list'))

    def test_tagged_list(self):
        owner1 = "ds_list_user7"
        owner2 = "ds_list_user8"
        owner3 = "ds_list_user9"
        self.remove_users.append(owner1)
        self.remove_users.append(owner2)
        self.remove_users.append(owner3)

        auth_headers = self.get_auth_header_for_username(owner1)
        auth_headers2 = self.get_auth_header_for_username(owner2)
        auth_headers3 = self.get_auth_header_for_username(owner3)
        url = reverse("sqlshare_view_dataset_tagged_list", kwargs={"tag": "__test_tag_api__" })

        ds1 = create_dataset_from_query(owner1, "ds_owned2", "SELECT(1)")
        ds2 = create_dataset_from_query(owner2, "ds_shared3", "SELECT(1)")
        ds3 = create_dataset_from_query(owner3, "ds_public2", "SELECT(1)")

        ds4 = create_dataset_from_query(owner1, "ds_owned_tagged", "SELECT(1)")
        ds5 = create_dataset_from_query(owner2, "ds_shared_tagged", "SELECT(1)")
        ds6 = create_dataset_from_query(owner3, "ds_public_tagged", "SELECT(1)")

        # Share the shared datasets...
        set_dataset_accounts(ds2, [ owner1 ])
        set_dataset_accounts(ds5, [ owner1 ])

        # Make dataset public
        add_public_access(ds3)
        add_public_access(ds6)

        def build_lookup(data):
            lookup = {}
            for item in data:
                owner = item["owner"]
                name = item["name"]
                if not owner in lookup:
                    lookup[owner] = {}
                lookup[owner][name] = True
            return lookup

        response = self.client.get(url, **auth_headers)

        # No tags added yet - should be an empty list
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data, [])

        # Add the tag to 3 datasets:
        tag_url = reverse("sqlshare_view_dataset_tags", kwargs={ 'owner': owner1, 'name': "ds_owned_tagged"})
        data = [ { "name": owner1, "tags": [ "tag1", "__test_tag_api__" ] } ]
        self.client.put(tag_url, data=json.dumps(data), **auth_headers)

        tag_url = reverse("sqlshare_view_dataset_tags", kwargs={ 'owner': owner2, 'name': "ds_shared_tagged"})
        data = [ { "name": owner2, "tags": [ "tag2", "__test_tag_api__" ] } ]
        self.client.put(tag_url, data=json.dumps(data), **auth_headers2)

        tag_url = reverse("sqlshare_view_dataset_tags", kwargs={ 'owner': owner3, 'name': "ds_public_tagged"})
        data = [ { "name": owner1, "tags": [ "tag3", "__test_tag_api__" ] } ]
        self.client.put(tag_url, data=json.dumps(data), **auth_headers)

        with LogCapture() as l:
            response = self.client.get(url, **auth_headers)
            data = json.loads(response.content.decode("utf-8"))
            lookup = build_lookup(data)
            self.assertTrue(lookup["ds_list_user7"]["ds_owned_tagged"])
            self.assertTrue(lookup["ds_list_user8"]["ds_shared_tagged"])
            self.assertTrue(lookup["ds_list_user9"]["ds_public_tagged"])
            self.assertTrue("ds_public" not in lookup["ds_list_user9"])
            self.assertTrue("ds_shared" not in lookup["ds_list_user8"])
            self.assertTrue("ds_owned" not in lookup["ds_list_user7"])

            self.assertTrue(self._has_log(l, "ds_list_user7", None, 'sqlshare_rest.views.dataset_list', 'INFO', 'GET tagged dataset list; tag: __test_tag_api__'))

        # Test case-insensitivity
        url = reverse("sqlshare_view_dataset_tagged_list", kwargs={"tag": "__TEST_TAG_API__" })
        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        lookup = build_lookup(data)
        self.assertTrue(lookup["ds_list_user7"]["ds_owned_tagged"])
        self.assertTrue(lookup["ds_list_user8"]["ds_shared_tagged"])
        self.assertTrue(lookup["ds_list_user9"]["ds_public_tagged"])
        self.assertTrue("ds_public" not in lookup["ds_list_user9"])
        self.assertTrue("ds_shared" not in lookup["ds_list_user8"])
        self.assertTrue("ds_owned" not in lookup["ds_list_user7"])

    def test_pagination(self):
        owner = "test_pagination_owner"
        public = "test_pagination_public"
        shared = "test_pagination_shared"
        self.remove_users.append(owner)
        self.remove_users.append(public)
        self.remove_users.append(shared)

        backend = get_backend()
        backend.get_user(public)
        backend.get_user(shared)
        auth_headers = self.get_auth_header_for_username(owner)
        public_auth_headers = self.get_auth_header_for_username(public)
        shared_auth_headers = self.get_auth_header_for_username(shared)

        never_seen = create_dataset_from_query(public, "test_paging_public_owner_first", "SELECT (1)")

        account_data = { "accounts": [ shared ] }
        for i in range(200):
            dataset_name = "test_paging_%s" % i
            ds = create_dataset_from_query(owner, dataset_name, "SELECT (%s)" % i)
            ds.is_public = True
            set_dataset_accounts(ds, [ shared ])

            if i < 120:
                ds.description = "Find the elephant"
            ds.save()

        url = reverse("sqlshare_view_dataset_list")

        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 200)

        response = self.client.get(url, { "page": 1, "page_size": 50, "order_by": "updated" }, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 50)
        self.assertEquals(data[0]["name"], "test_paging_199")

        response = self.client.get(url, { "page": 2, "page_size": 50, "order_by": "updated" }, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 50)
        self.assertEquals(data[0]["name"], "test_paging_149")

        response = self.client.get(url, { "page": 100, "page_size": 50, "order_by": "updated" }, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 0)

        response = self.client.get(url, { "page": 10, "page_size": 10, "order_by": "updated" }, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 10)

        response = self.client.get(url, { "page": 1 }, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 50)
        self.assertEquals(data[0]["name"], "test_paging_0")

        url = reverse("sqlshare_view_dataset_shared_list")

        response = self.client.get(url, **shared_auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 200)

        response = self.client.get(url, { "page": 1, "page_size": 50, "order_by": "updated" }, **shared_auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 50)
        self.assertEquals(data[0]["name"], "test_paging_199")

        url = reverse("sqlshare_view_dataset_all_list")

        response = self.client.get(url, **public_auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertTrue(len(data) >= 200)

        response = self.client.get(url, { "page": 1, "page_size": 50, "order_by": "updated" }, **public_auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 50)
        self.assertEquals(data[0]["name"], "test_paging_199")

        new_public = create_dataset_from_query(public, "test_paging_public_owner", "SELECT (1)")
        response = self.client.get(url, { "page": 1, "page_size": 50, "order_by": "updated" }, **public_auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 50)
        self.assertEquals(data[0]["name"], "test_paging_public_owner")
        self.assertEquals(data[1]["name"], "test_paging_199")

        # Now for searching...
        response = self.client.get(url, { "q": "elephant" }, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 120)

        response = self.client.get(url, { "q": "elephant", "page": 1, "page_size": 50, "order_by": "updated" }, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 50)
        self.assertEquals(data[0]["name"], "test_paging_119")

    def test_recent_datasets(self):
        owner = "test_recents_owner"
        public = "test_recents_public"

        self.remove_users.append(owner)
        self.remove_users.append(public)

        ds1 = create_dataset_from_query(owner, "recent_ds1", "SELECT (1)")
        ds2 = create_dataset_from_query(owner, "recent_ds2", "SELECT (1)")

        add_public_access(ds1)
        add_public_access(ds2)

        url = reverse("sqlshare_view_dataset_recent_list")

        auth_headers = self.get_auth_header_for_username(owner)
        public_auth_headers = self.get_auth_header_for_username(public)

        # initial state - neither user has visited any dataset
        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 0)

        response = self.client.get(url, **public_auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 0)

        # Test public user ds1/ds2 - still no items for the owner
        ds1_url = reverse("sqlshare_view_dataset", kwargs={ 'owner': owner,
                                                            'name': "recent_ds1"})
        ds2_url = reverse("sqlshare_view_dataset", kwargs={ 'owner': owner,
                                                            'name': "recent_ds2"})

        response = self.client.get(ds1_url, **public_auth_headers)
        sleep(1.1)
        response = self.client.get(ds2_url, **public_auth_headers)

        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 0)

        response = self.client.get(url, **public_auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 2)

        self.assertEquals(data[0]["name"], "recent_ds2")
        self.assertEquals(data[1]["name"], "recent_ds1")

        response = self.client.get(ds2_url, **auth_headers)
        sleep(1.1)
        response = self.client.get(ds1_url, **auth_headers)

        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 2)
        self.assertEquals(data[0]["name"], "recent_ds1")
        self.assertEquals(data[1]["name"], "recent_ds2")

        response = self.client.get(url, **public_auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 2)

        self.assertEquals(data[0]["name"], "recent_ds2")
        self.assertEquals(data[1]["name"], "recent_ds1")

        # Go and visit #1 again, make sure we have things in the right order.
        sleep(1.1)
        response = self.client.get(ds2_url, **auth_headers)
        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 2)
        self.assertEquals(data[0]["name"], "recent_ds2")
        self.assertEquals(data[1]["name"], "recent_ds1")

        response = self.client.get(url, { "q": "ds2", "page": 1, "page_size": 50, "order_by": "updated" }, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 1)
        self.assertEquals(data[0]["name"], "recent_ds2")



    @classmethod
    def setUpClass(cls):
        def _run_query(sql):
            cursor = connection.cursor()
            try:
                cursor.execute(sql)
            except Exception as ex:
                # Hopefully all of these will fail, so ignore the failures
                pass

        # This is just an embarrassing list of things to cleanup if something fails.
        # It gets added to when something like this blocks one of my test runs...
        _run_query("drop login ds_list_user8")
        _run_query("drop login ds_list_user9")
        _run_query("drop login ds_list_user6")

