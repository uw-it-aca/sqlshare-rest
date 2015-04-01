from django.test import TestCase
from unittest2 import skipIf
from django.db import connection
import json
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.test import missing_url
from django.test.utils import override_settings
from django.test.client import Client
from django.core.urlresolvers import reverse
from sqlshare_rest.test.api.base import BaseAPITest
from sqlshare_rest.dao.dataset import create_dataset_from_query

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

class DatasetPermissionsAPITest(BaseAPITest):
    def setUp(self):
        # Try to cleanup from any previous test runs...
        self.remove_users = []
        self.client = Client()

    def test_unauthenticated(self):
        url = reverse("sqlshare_view_dataset_permissions", kwargs={"owner":"foo", "name":"bar"})
        response = self.client.get(url)
        self.assertEquals(response.status_code, 403)

    def test_accounts(self):
        owner = "permissions_user1"
        dataset_name = "ds1"
        other_user1 = "permissions_user2"
        other_user2 = "permissions_user3"
        other_user3 = "permissions_user4"
        self.remove_users.append(owner)
        self.remove_users.append(other_user1)
        self.remove_users.append(other_user2)
        self.remove_users.append(other_user3)

        backend = get_backend()
        backend.get_user(other_user1)
        backend.get_user(other_user2)
        backend.get_user(other_user3)
        ds1 = create_dataset_from_query(owner, dataset_name, "SELECT(1)")

        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': owner,
                                                        'name': dataset_name})

        owner_auth_headers = self.get_auth_header_for_username(owner)
        user1_auth_headers = self.get_auth_header_for_username(other_user1)
        user2_auth_headers = self.get_auth_header_for_username(other_user2)
        user3_auth_headers = self.get_auth_header_for_username(other_user3)

        # Test the default situation...
        response = self.client.get(url, **owner_auth_headers)
        self.assertEquals(response.status_code, 200)
        response = self.client.get(url, **user1_auth_headers)
        self.assertEquals(response.status_code, 403)
        response = self.client.get(url, **user2_auth_headers)
        self.assertEquals(response.status_code, 403)
        response = self.client.get(url, **user3_auth_headers)
        self.assertEquals(response.status_code, 403)

        # Test the default state of the permissions api...
        permissions_url = reverse("sqlshare_view_dataset_permissions", kwargs={'owner':owner, 'name':dataset_name})
        response = self.client.get(permissions_url, **owner_auth_headers)
        self.assertEquals(response.status_code, 200)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["is_public"], False)
        self.assertEquals(data["is_shared"], False)
        self.assertEquals(data["accounts"], [])
        self.assertEquals(data["emails"], [])

        # Test round 1 of changes...
        new_data = { "accounts": [ other_user1, other_user2 ] }
        response = self.client.put(permissions_url, data=json.dumps(new_data), **user1_auth_headers)
        self.assertEquals(response.status_code, 403)

        response = self.client.put(permissions_url, data=json.dumps(new_data), **owner_auth_headers)
        self.assertEquals(response.status_code, 200)
        self.assertEquals(response.content.decode("utf-8"), "")

        response = self.client.get(permissions_url, **owner_auth_headers)
        self.assertEquals(response.status_code, 200)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["is_public"], False)
        self.assertEquals(data["is_shared"], True)
        self.assertEquals(data["emails"], [])

        accounts = data["accounts"]
        lookup = {}
        for account in accounts:
            lookup[account["login"]] = account

        self.assertTrue(other_user1 in lookup)
        self.assertTrue(other_user2 in lookup)
        self.assertFalse(other_user3 in lookup)

        self.assertEquals(lookup[other_user1]["login"], other_user1)
        self.assertEquals(lookup[other_user2]["login"], other_user2)

        # Make sure they can get the dataset...
        response = self.client.get(url, **owner_auth_headers)
        self.assertEquals(response.status_code, 200)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["is_shared"], True)

        response = self.client.get(url, **user1_auth_headers)
        self.assertEquals(response.status_code, 200)
        response = self.client.get(permissions_url, **user1_auth_headers)
        self.assertEquals(response.status_code, 403)
        response = self.client.get(url, **user2_auth_headers)
        self.assertEquals(response.status_code, 200)
        response = self.client.get(url, **user3_auth_headers)
        self.assertEquals(response.status_code, 403)

        # Test round 2 of changes... add a new user, drop a user
        new_data = { "accounts": [ other_user3, other_user2 ] }
        response = self.client.put(permissions_url, data=json.dumps(new_data), **user1_auth_headers)
        self.assertEquals(response.status_code, 403)

        response = self.client.put(permissions_url, data=json.dumps(new_data), **owner_auth_headers)
        self.assertEquals(response.status_code, 200)
        self.assertEquals(response.content.decode("utf-8"), "")

        response = self.client.get(permissions_url, **owner_auth_headers)
        self.assertEquals(response.status_code, 200)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["is_public"], False)
        self.assertEquals(data["is_shared"], True)
        self.assertEquals(data["emails"], [])

        accounts = data["accounts"]
        lookup = {}
        for account in accounts:
            lookup[account["login"]] = account

        self.assertTrue(other_user3 in lookup)
        self.assertTrue(other_user2 in lookup)
        self.assertFalse(other_user1 in lookup)

        self.assertEquals(lookup[other_user3]["login"], other_user3)
        self.assertEquals(lookup[other_user2]["login"], other_user2)

        # Make sure they can get the dataset...
        response = self.client.get(url, **owner_auth_headers)
        self.assertEquals(response.status_code, 200)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["is_shared"], True)

        response = self.client.get(url, **user1_auth_headers)
        self.assertEquals(response.status_code, 403)
        response = self.client.get(url, **user2_auth_headers)
        self.assertEquals(response.status_code, 200)
        response = self.client.get(url, **user3_auth_headers)
        self.assertEquals(response.status_code, 200)

        # Test round 3 of changes... remove all acces
        new_data = { "accounts": [] }
        response = self.client.put(permissions_url, data=json.dumps(new_data), **user1_auth_headers)
        self.assertEquals(response.status_code, 403)

        response = self.client.put(permissions_url, data=json.dumps(new_data), **owner_auth_headers)
        self.assertEquals(response.status_code, 200)
        self.assertEquals(response.content.decode("utf-8"), "")

        response = self.client.get(permissions_url, **owner_auth_headers)
        self.assertEquals(response.status_code, 200)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["is_public"], False)
        self.assertEquals(data["is_shared"], False)
        self.assertEquals(data["emails"], [])
        self.assertEquals(data["accounts"], [])

        # Make sure they can get the dataset...
        response = self.client.get(url, **owner_auth_headers)
        self.assertEquals(response.status_code, 200)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["is_shared"], False)

        response = self.client.get(url, **user1_auth_headers)
        self.assertEquals(response.status_code, 403)
        response = self.client.get(url, **user2_auth_headers)
        self.assertEquals(response.status_code, 403)
        response = self.client.get(url, **user3_auth_headers)
        self.assertEquals(response.status_code, 403)
