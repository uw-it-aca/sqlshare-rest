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

class DatsetAPITest(BaseAPITest):
    def setUp(self):
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
        response = self.client.get(url, **auth_headers)

        self.assertEquals(response.content.decode("utf-8"), '[]')

    def test_no_description(self):
        """
        The dataset api needs to always have a description string, even if
        the model value is null.
        """
        owner = "no_description_owner"
        self.remove_users.append(owner)
        auth_headers = self.get_auth_header_for_username(owner)

        ds1 = create_dataset_from_query(owner, "ds1", "SELECT(1)")
        # Should be None anyway, but why not...
        ds1.description = None
        ds1.save()

        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': owner,
                                                        'name': "ds1"})

        response = self.client.get(url, **auth_headers)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["description"], "")


    def test_popularity(self):
        owner = "mr_popular"
        other = "other_account"
        self.remove_users.append(owner)
        self.remove_users.append(other)
        auth_headers = self.get_auth_header_for_username(owner)
        other_auth_headers = self.get_auth_header_for_username(other)

        ds1 = create_dataset_from_query(owner, "ds1", "SELECT(1)")
        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': owner,
                                                        'name': "ds1"})

        permissions_url = reverse("sqlshare_view_dataset_permissions", kwargs={'owner':owner, 'name':"ds1"})

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

        self.assertTrue((now - cd_obj).total_seconds() < 2)
        self.assertTrue((now - md_obj).total_seconds() < 2)

        self.assertTrue((cd_obj - now).total_seconds() > -2)
        self.assertTrue((md_obj - now).total_seconds() > -2)

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

        process_queue()
        # Test that the GET returns data after the second PUT too...
        response = self.client.get(url, **auth_headers)
        self.assertEquals(response.status_code, 200)

        data = json.loads(response.content.decode("utf-8"))

        self.assertEquals(data["owner"], owner)
        self.assertEquals(data["description"], "This is a test dataset")


    def test_valid_no_permissions(self):
        owner = "put_user2"
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
        response = self.client.put(url, data=json_data, **auth_headers)
        self.assertEquals(response.status_code, 201)

        auth_headers = self.get_auth_header_for_username("not_owner")

        response = self.client.get(url, **auth_headers)

        self.assertEquals(response.status_code, 403)

    def test_public_access(self):
        owner = "put_user3"
        ds1_name = "dataset_1"
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
