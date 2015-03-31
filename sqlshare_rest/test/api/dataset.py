from django.test import TestCase
from unittest2 import skipIf
from django.db import connection
import json
from sqlshare_rest.test import missing_url
from django.test.utils import override_settings
from django.test.client import Client
from django.core.urlresolvers import reverse
from sqlshare_rest.test.api.base import BaseAPITest

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
        auth_headers = self.get_auth_header_for_username("test_user1")
        url = reverse("sqlshare_view_dataset_list")
        response = self.client.get(url, **auth_headers)

        self.assertEquals(response.content.decode("utf-8"), '[]')

    def test_get_missing(self):
        owner = "okwhateveruser"
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
        self.assertEquals(data["tags"], [])
        self.assertEquals(data["url"], url)

        # Test that the GET returns data too...
        response = self.client.get(url, **auth_headers)
        self.assertEquals(response.status_code, 200)

        data = json.loads(response.content.decode("utf-8"))

        self.assertEquals(data["sample_data_status"], "working")
        self.assertEquals(data["description"], "This is a test dataset")
        self.assertEquals(data["is_public"], False)
        self.assertEquals(data["is_shared"], False)
        self.assertEquals(data["sql_code"], "SELECT(1)")
        self.assertEquals(data["columns"], None)
        self.assertEquals(data["popularity"], 0)
        self.assertEquals(data["tags"], [])
        self.assertEquals(data["url"], url)


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
        _run_query("drop user meta_3a95151f1de")
        _run_query("drop user meta_8af92476928")
        _run_query("drop user meta_012da3777ee")
