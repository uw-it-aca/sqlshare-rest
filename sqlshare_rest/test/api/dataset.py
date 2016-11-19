from django.test import TestCase
from unittest2 import skipIf
from django.db import connection
import json
from datetime import datetime, timedelta
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

class DatsetAPITest(BaseAPITest):
    def setUp(self):
        super(DatsetAPITest, self).setUp()
        # Try to cleanup from any previous test runs...
        self.remove_users = []
        self.client = Client()

    def test_unauthenticated(self):
        url = reverse("sqlshare_view_dataset_list")
        response = self.client.get(url)
        self.assertEquals(response.status_code, 403)

    def test_methods(self):
        self.remove_users.append("test_user1")
        auth_headers = self.get_auth_header_for_username("test_user1")
        url = reverse("sqlshare_view_dataset_list")
        with LogCapture() as l:
            response = self.client.get(url, **auth_headers)

            self.assertEquals(response.content.decode("utf-8"), '[]')
            self.assertTrue(self._has_log(l, "test_user1", None, 'sqlshare_rest.views.dataset_list', 'INFO', "GET my dataset list; search ''; order_by: 'updated'; page_num: 1, page_size: 50"))

    def test_no_description(self):
        """
        The dataset api needs to always have a description string, even if
        the model value is null.
        """
        owner = "no_description_owner"
        self.remove_users.append(owner)
        auth_headers = self.get_auth_header_for_username(owner)

        ds1 = create_dataset_from_query(owner, "ds9", "SELECT(1)")
        # Should be None anyway, but why not...
        ds1.description = None
        ds1.save()

        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': owner,
                                                        'name': "ds9"})

        with LogCapture() as l:
            response = self.client.get(url, **auth_headers)
            data = json.loads(response.content.decode("utf-8"))
            self.assertEquals(data["description"], "")

            self.assertTrue(self._has_log(l, owner, None, 'sqlshare_rest.views.dataset', 'INFO', 'GET dataset; owner: no_description_owner; name: ds9'))

    def test_popularity(self):
        owner = "mr_popular"
        other = "other_account"
        self.remove_users.append(owner)
        self.remove_users.append(other)
        auth_headers = self.get_auth_header_for_username(owner)
        other_auth_headers = self.get_auth_header_for_username(other)

        ds1 = create_dataset_from_query(owner, "ds10", "SELECT(1)")
        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': owner,
                                                        'name': "ds10"})

        permissions_url = reverse("sqlshare_view_dataset_permissions", kwargs={'owner':owner, 'name':"ds10"})

        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["popularity"], 1)

        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["popularity"], 2)

        # Test the someone w/o access doesn't increase the popularity
        response = self.client.get(url, **other_auth_headers)
        self.assertEquals(response.status_code, 403)

        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["popularity"], 3)

        get_backend().get_user(other)
        # Give them access, make sure it increases popularity
        new_data = { "accounts": [ other ] }
        response = self.client.put(permissions_url, data=json.dumps(new_data), **auth_headers)
        self.assertEquals(response.status_code, 200)
        self.assertEquals(response.content.decode("utf-8"), "")

        response = self.client.get(url, **other_auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["popularity"], 4)


    def test_get_missing(self):
        owner = "okwhateveruser"
        self.remove_users.append(owner)
        ds1_name = "not-really-here"
        auth_headers = self.get_auth_header_for_username(owner)

        # Valid user, no dataset
        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': owner,
                                                        'name': ds1_name})

        response = self.client.get(url, **auth_headers)
        self.assertEquals(response.status_code, 404)

        # Not a valid user
        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': "new_made_up",
                                                        'name': ds1_name})

        response = self.client.get(url, **auth_headers)
        self.assertEquals(response.status_code, 404)


    def test_create_from_query(self):
        owner = "put_user1"
        ds1_name = "dataset_1"
        self.remove_users.append(owner)
        auth_headers = self.get_auth_header_for_username(owner)
        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': owner,
                                                        'name': ds1_name})

        data = {
            "sql_code": "SELECT(1)",
            "is_public": False,
            "is_snapshot": False,
            "description": "This is a test dataset",
        }

        json_data = json.dumps(data)

        # Test the right response from the PUT
        with LogCapture() as l:
            response = self.client.put(url, data=json_data, **auth_headers)
            self.assertEquals(response.status_code, 201)

            data = json.loads(response.content.decode("utf-8"))

            self.assertEquals(data["sample_data_status"], "working")
            self.assertEquals(data["description"], "This is a test dataset")
            self.assertEquals(data["is_public"], False)
            self.assertEquals(data["is_shared"], False)
            self.assertEquals(data["sql_code"], "SELECT(1)")
            self.assertEquals(data["columns"], None)
            self.assertEquals(data["popularity"], 0)
            self.assertEquals(data["name"], ds1_name)
            self.assertEquals(data["tags"], [])
            self.assertEquals(data["url"], url)

            creation_date = data["date_created"]
            modification_date = data["date_modified"]

            cd_obj = parser.parse(creation_date)
            md_obj = parser.parse(modification_date)

            now = timezone.now()
            limit = get_backend().get_testing_time_delta_limit()

            self.assertTrue((now - cd_obj).total_seconds() < limit)
            self.assertTrue((now - md_obj).total_seconds() < limit)

            self.assertTrue((cd_obj - now).total_seconds() > -1 * limit)
            self.assertTrue((md_obj - now).total_seconds() > -1 * limit)

            self.assertTrue(self._has_log(l, owner, None, 'sqlshare_rest.views.dataset', 'INFO', 'PUT dataset; owner: put_user1; name: dataset_1'))

        # Test that the GET returns data too...
        response = self.client.get(url, **auth_headers)
        self.assertEquals(response.status_code, 200)

        data = json.loads(response.content.decode("utf-8"))

        self.assertEquals(data["sample_data_status"], "working")
        self.assertEquals(data["owner"], owner)
        self.assertEquals(data["description"], "This is a test dataset")
        self.assertEquals(data["is_public"], False)
        self.assertEquals(data["is_shared"], False)
        self.assertEquals(data["sql_code"], "SELECT(1)")
        self.assertEquals(data["columns"], None)
        self.assertEquals(data["popularity"], 1)
        self.assertEquals(data["tags"], [])
        self.assertEquals(data["url"], url)

    def test_repeated_puts(self):
        """
        This *should* update the dataset initially created
        """
        Query.objects.all().delete()
        owner = "put_user1a"
        ds1_name = "dataset_1a"
        self.remove_users.append(owner)
        auth_headers = self.get_auth_header_for_username(owner)
        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': owner,
                                                        'name': ds1_name})

        data = {
            "sql_code": "SELECT(1)",
            "is_public": False,
            "is_snapshot": False,
            "description": "This is a test dataset",
        }

        json_data = json.dumps(data)

        # Test the right response from the PUT
        response = self.client.put(url, data=json_data, **auth_headers)
        self.assertEquals(response.status_code, 201)

        query = Query.objects.all()[0]
        remove_pk = query.pk
        process_queue()
        data = json.loads(response.content.decode("utf-8"))

        self.assertEquals(data["name"], ds1_name)


        # Test that the GET returns data too...
        response = self.client.get(url, **auth_headers)
        self.assertEquals(response.status_code, 200)

        # Make sure we have a new schema:
        data["sql_code"] = "SELECT (1), (2)"

        response = self.client.put(url, data=json_data, **auth_headers)
        self.assertEquals(response.status_code, 201)

        response = self.client.get(url, **auth_headers)
        self.assertEquals(response.status_code, 200)

        data = json.loads(response.content.decode("utf-8"))

        self.assertEquals(data["sample_data_status"], "working")


        query = Query.objects.all()[0]
        remove_pk2 = query.pk
        process_queue()
        # Test that the GET returns data after the second PUT too...
        response = self.client.get(url, **auth_headers)
        self.assertEquals(response.status_code, 200)

        data = json.loads(response.content.decode("utf-8"))

        self.assertEquals(data["owner"], owner)
        self.assertEquals(data["sample_data_status"], "success")
        self.assertEquals(data["description"], "This is a test dataset")

    def test_repeated_puts_no_queue_run(self):
        """
        This *should* update the dataset initially created
        """
        Query.objects.all().delete()
        owner = "put_user1b"
        ds1_name = "dataset_1b"
        self.remove_users.append(owner)
        auth_headers = self.get_auth_header_for_username(owner)
        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': owner,
                                                        'name': ds1_name})

        data = {
            "sql_code": "SELECT(1)",
            "is_public": False,
            "is_snapshot": False,
            "description": "This is a test dataset",
        }

        json_data = json.dumps(data)

        # Test the right response from the PUT
        response = self.client.put(url, data=json_data, **auth_headers)
        self.assertEquals(response.status_code, 201)

        data = json.loads(response.content.decode("utf-8"))

        self.assertEquals(data["name"], ds1_name)


        # Test that the GET returns data too...
        response = self.client.get(url, **auth_headers)
        self.assertEquals(response.status_code, 200)

        # Make sure we have a new schema:
        data["sql_code"] = "SELECT (1), (2)"

        response = self.client.put(url, data=json_data, **auth_headers)
        self.assertEquals(response.status_code, 201)

        query = Query.objects.all()[0]
        remove_pk = query.pk
        process_queue()
        # Test that the GET returns data after the second PUT too...
        response = self.client.get(url, **auth_headers)
        self.assertEquals(response.status_code, 200)

        data = json.loads(response.content.decode("utf-8"))

        self.assertEquals(data["owner"], owner)
        self.assertEquals(data["description"], "This is a test dataset")

    def test_valid_no_permissions(self):
        owner = "put_user2"
        ds1_name = "dataset_1c"
        self.remove_users.append(owner)
        self.remove_users.append("not_owner")
        auth_headers = self.get_auth_header_for_username(owner)
        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': owner,
                                                        'name': ds1_name})

        data = {
            "sql_code": "SELECT(1)",
            "is_public": False,
            "is_snapshot": False,
            "description": "This is a test dataset",
        }

        json_data = json.dumps(data)

        # Test the right response from the PUT
        response = self.client.put(url, data=json_data, **auth_headers)
        self.assertEquals(response.status_code, 201)

        auth_headers = self.get_auth_header_for_username("not_owner")

        response = self.client.get(url, **auth_headers)

        self.assertEquals(response.status_code, 403)

    def test_public_access(self):
        owner = "put_user3"
        ds1_name = "dataset_1d"
        self.remove_users.append(owner)
        auth_headers = self.get_auth_header_for_username(owner)
        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': owner,
                                                        'name': ds1_name})

        data = {
            "sql_code": "SELECT(1)",
            "is_public": True,
            "is_snapshot": False,
            "description": "This is a test dataset",
        }

        json_data = json.dumps(data)

        # Test the right response from the PUT
        response = self.client.put(url, data=json_data, **auth_headers)
        self.assertEquals(response.status_code, 201)

        auth_headers = self.get_auth_header_for_username("not_owner")

        response = self.client.get(url, **auth_headers)

        self.assertEquals(response.status_code, 200)

    def test_patch(self):
        owner = "patch_adams"
        self.remove_users.append(owner)
        auth_headers = self.get_auth_header_for_username(owner)

        ds1 = create_dataset_from_query(owner, "ds11", "SELECT(1)")
        # Should be None anyway, but why not...
        ds1.description = None
        ds1.save()

        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': owner,
                                                        'name': "ds11"})

        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["description"], "")

        with LogCapture() as l:
            self.client.patch(url, '{"description": "VIA PATCH"}', content_type="application/json", **auth_headers)

            response = self.client.get(url, **auth_headers)
            data = json.loads(response.content.decode("utf-8"))
            self.assertEquals(data["description"], "VIA PATCH")
            self.assertTrue(self._has_log(l, owner, None, 'sqlshare_rest.views.dataset', 'INFO', 'PATCH dataset description; owner: patch_adams; name: ds11; description: VIA PATCH'))


        self.client.patch(url, '{"rando-field": "MEH"}', content_type="application/json", **auth_headers)

        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["description"], "VIA PATCH")

        self.client.patch(url, '{"description": null}', content_type="application/json", **auth_headers)

        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["description"], "")

        ds1 = Dataset.objects.get(pk = ds1.pk)
        self.assertFalse(ds1.is_public)

        with LogCapture() as l:
            self.client.patch(url, '{"is_public": true }', content_type="application/json", **auth_headers)
            ds1 = Dataset.objects.get(pk = ds1.pk)
            self.assertTrue(ds1.is_public)
            self.assertTrue(self._has_log(l, owner, None, 'sqlshare_rest.views.dataset', 'INFO', 'PATCH dataset is_public; owner: patch_adams; name: ds11; is_public: True'))

        self.client.patch(url, '{"rando-2": true }', content_type="application/json", **auth_headers)
        ds1 = Dataset.objects.get(pk = ds1.pk)
        self.assertTrue(ds1.is_public)

        with LogCapture() as l:
            self.client.patch(url, '{"is_public": false }', content_type="application/json", **auth_headers)
            ds1 = Dataset.objects.get(pk = ds1.pk)
            self.assertFalse(ds1.is_public)
            self.assertTrue(self._has_log(l, owner, None, 'sqlshare_rest.views.dataset', 'INFO', 'PATCH dataset is_public; owner: patch_adams; name: ds11; is_public: False'))

        # Check that the SQL code can be patched, and that we get a new preview
        Query.objects.all().delete()

        with LogCapture() as l:
            response = self.client.patch(url, '{"sql_code":"SELECT(2)"}', content_type="application/json", **auth_headers)
            ds1 = Dataset.objects.get(pk = ds1.pk)
            self.assertEquals(ds1.sql, "SELECT(2)")
            self.assertEquals(ds1.preview_is_finished, False)
            self.assertTrue(self._has_log(l, owner, None, 'sqlshare_rest.views.dataset', 'INFO', 'PATCH dataset sql_code; owner: patch_adams; name: ds11; sql_code: SELECT(2)'))

        query = Query.objects.all()[0]
        remove_pk = query.pk
        process_queue()
        ds1 = Dataset.objects.get(pk = ds1.pk)
        self.assertEquals(ds1.preview_is_finished, True)
        self.assertEquals(ds1.preview_error, None)

    def test_delete(self):
        owner = "test_dataset_delete"
        self.remove_users.append(owner)
        auth_headers = self.get_auth_header_for_username(owner)

        ds1 = create_dataset_from_query(owner, "ds12", "SELECT(1)")
        # Should be None anyway, but why not...
        ds1.description = None
        ds1.save()

        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': owner,
                                                        'name': "ds12"})

        with LogCapture() as l:
            response = self.client.delete(url, **auth_headers)
            self.assertEquals(response.status_code, 200)
            self.assertTrue(self._has_log(l, owner, None, 'sqlshare_rest.views.dataset', 'INFO', 'DELETE dataset; owner: test_dataset_delete; name: ds12'))
                

        response = self.client.get(url, **auth_headers)
        self.assertEquals(response.status_code, 404)

    def test_download(self):
        owner = "test_dataset_download"
        self.remove_users.append(owner)
        auth_headers = self.get_auth_header_for_username(owner)

        ds1 = create_dataset_from_query(owner, "ds13", "SELECT(1)")
        # Should be None anyway, but why not...

        url = reverse("sqlshare_view_download_dataset", kwargs={ 'owner': owner,
                                                                 'name': "ds13"})

        response = self.client.get(url, **auth_headers)
        self.assertEquals(response.status_code, 405)

        response = self.client.post(url, {}, **auth_headers)
        self.assertEquals(response.status_code, 200)

        self.assertEquals(response["Content-Disposition"],  'attachment; filename="ds13.csv"')
        self.assertEquals(response["Content-Type"],  'text/csv')

        data = StringIO("".join(map(lambda x: x.decode("utf-8"), response.streaming_content)))
        reader = csv.reader(data, delimiter=",")
        values = []
        for row in reader:
            values.append(row)

        self.assertEquals(len(values), 2)
        self.assertEquals(values[1][0], "1")

    def test_get_and_timestamps(self):
        owner = "ds_owner_timestamp_test"
        self.remove_users.append(owner)
        ds1 = create_dataset_from_query(owner, "ds_timestamp1", "SELECT (1)")

        my_date = timezone.now() - timedelta(days=30)
        my_date_str = my_date.strftime("%D %T")

        ds1.date_modified = my_date
        ds1.save()

        ds1 = Dataset.objects.get(pk=ds1.pk)
        self.assertEquals(ds1.date_modified.strftime("%D %T"), my_date_str)

        now = timezone.now() - timedelta(seconds=1)
        future = now + timedelta(minutes=1)

        ds1.description = "new timestamp description"
        ds1.save()

        ds1 = Dataset.objects.get(pk=ds1.pk)
        self.assertTrue(ds1.date_modified >= now)
        self.assertTrue(ds1.date_modified <= future)

        self.assertTrue(ds1.should_update_modified_date("description"))
        self.assertTrue(ds1.should_update_modified_date("is_public"))
        self.assertTrue(ds1.should_update_modified_date("is_shared"))
        self.assertTrue(ds1.should_update_modified_date("shared_with"))
        self.assertTrue(ds1.should_update_modified_date("sql"))

        self.assertFalse(ds1.should_update_modified_date("popularity"))
        self.assertFalse(ds1.should_update_modified_date("last_viewed"))
        self.assertFalse(ds1.should_update_modified_date("preview_is_finished"))

    @classmethod
    def setUpClass(cls):
        super(DatsetAPITest, cls).setUpClass()
        def _run_query(sql):
            cursor = connection.cursor()
            try:
                cursor.execute(sql)
            except Exception as ex:
                # Hopefully all of these will fail, so ignore the failures
                pass

        # This is just an embarrassing list of things to cleanup if something fails.
        # It gets added to when something like this blocks one of my test runs...
        _run_query("drop database put_user1")
        _run_query("drop database put_user2")
        _run_query("drop database put_user3")
        _run_query("drop database dao_user1")
        _run_query("drop database test_user1")
        _run_query("drop database okwhateveruser")
        _run_query("drop user meta_3a95151f1de")
        _run_query("drop user meta_8af92476928")
        _run_query("drop user meta_012da3777ee")
        _run_query("drop user meta_e1bc449093c")
        _run_query("drop user meta_9e311190103")
        _run_query("drop login put_user1")
        _run_query("drop login ds_owner_timestamp_test")
        
