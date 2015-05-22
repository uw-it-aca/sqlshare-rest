from django.core.urlresolvers import reverse
from unittest2 import skipIf
from django.test import RequestFactory
from django.test.utils import override_settings
from django.test.client import Client
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.test.api.base import BaseAPITest
from sqlshare_rest.test import missing_url
from sqlshare_rest.dao.dataset import create_dataset_from_query
from sqlshare_rest.dao.dataset import set_dataset_accounts
from sqlshare_rest.models import User
from sqlshare_rest.dao.user import get_user
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

class UserOverrideAPITest(BaseAPITest):
    def setUp(self):
        # Try to cleanup from any previous test runs...
        self.remove_users = []
        self.client = Client()


    def test_user_api(self):
        self.remove_users = []
        user = "overrider"
        self.remove_users.append(user)
        self.remove_users.append("over2")
        user_auth_headers = self.get_auth_header_for_username(user)

        backend = get_backend()
        user_obj = backend.get_user(user)
        self._clear_override(user_obj)

        url = reverse("sqlshare_view_user")

        response = self.client.get(url, **user_auth_headers)
        self.assertEquals(response.status_code, 200)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["username"], user)
        self.assertEquals(data["schema"], user)


        user2 = backend.get_user("over2")
        self._override(user_obj, user2)

        response = self.client.get(url, **user_auth_headers)
        self.assertEquals(response.status_code, 200)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(data["username"], "over2")
        self.assertEquals(data["schema"], "over2")


    def test_dataset_api(self):
        self.remove_users = []
        user = "overrider"
        self.remove_users.append(user)
        self.remove_users.append("over2")
        user_auth_headers = self.get_auth_header_for_username(user)

        backend = get_backend()
        user_obj = backend.get_user(user)
        self._clear_override(user_obj)

        # Make sure we have the user we think...
        ds_overrider_1 = create_dataset_from_query(user, "ds_overrider_1", "SELECT (1)")
        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': user,
                                                        'name': "ds_overrider_1"})

        response = self.client.get(url, **user_auth_headers)
        self.assertEquals(response.status_code, 200)

        user2 = backend.get_user("over2")
        self._override(user_obj, user2)

        # Now test get as someone else.
        response = self.client.get(url, **user_auth_headers)
        self.assertEquals(response.status_code, 403)

        data = {
            "sql_code": "SELECT('FAIL')",
            "is_public": False,
            "is_snapshot": False,
            "description": "This is a test dataset",
        }

        json_data = json.dumps(data)

        # Test the right response from the PUT
        self.assertRaisesRegexp(Exception, "Owner doesn't match user: .*", self.client.put, url, data=json_data, **user_auth_headers)

        # Test the right response from the PATCH
        self.assertRaisesRegexp(Exception, "Owner doesn't match user: .*", self.client.patch, url, data=json_data, **user_auth_headers)

        # Test the right response from the DELETE
        self.assertRaisesRegexp(Exception, "Owner doesn't match user: .*", self.client.delete, url, data=json_data, **user_auth_headers)

        url = reverse("sqlshare_view_download_dataset", kwargs={ 'owner': user,
                                                                 'name': "ds_overrider_1"})
        response = self.client.post(url, **user_auth_headers)
        self.assertEquals(response.status_code, 403)

    def test_dataset_list_owned(self):
        self.remove_users = []
        user = "overrider_owner_list"
        self.remove_users.append(user)
        self.remove_users.append("over2")
        auth_headers = self.get_auth_header_for_username(user)

        backend = get_backend()
        user_obj = backend.get_user(user)
        self._clear_override(user_obj)

        ds_overrider_1 = create_dataset_from_query(user, "ds_overrider_list1", "SELECT (1)")
        url = reverse("sqlshare_view_dataset_list")
        response = self.client.get(url, **auth_headers)

        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 1)

        user2 = backend.get_user("over2")
        self._override(user_obj, user2)
        response = self.client.get(url, **auth_headers)

        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 0)

    def test_dataset_list_shared_with(self):
        self.remove_users = []
        user = "overrider_owner_sharer"
        self.remove_users.append(user)
        self.remove_users.append("overrider_recipient")
        self.remove_users.append("over2")

        backend = get_backend()
        backend.get_user(user)
        user_obj = backend.get_user("overrider_recipient")
        auth_headers = self.get_auth_header_for_username("overrider_recipient")
        self._clear_override(user_obj)

        ds_overrider_1 = create_dataset_from_query(user, "ds_overrider_list2", "SELECT (1)")


        set_dataset_accounts(ds_overrider_1, [ "overrider_recipient" ])
        url = reverse("sqlshare_view_dataset_shared_list")
        response = self.client.get(url, **auth_headers)

        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 1)

        user2 = backend.get_user("over2")
        self._override(user_obj, user2)

        response = self.client.get(url, **auth_headers)

        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 0)

    def test_dataset_list_all(self):
        self.remove_users = []
        user = "overrider_owner_list_all"
        self.remove_users.append(user)
        self.remove_users.append("over2")
        auth_headers = self.get_auth_header_for_username(user)

        backend = get_backend()
        user_obj = backend.get_user(user)
        self._clear_override(user_obj)

        ds_overrider_1 = create_dataset_from_query(user, "ds_overrider_list3", "SELECT (1)")
        url = reverse("sqlshare_view_dataset_all_list")
        response = self.client.get(url, **auth_headers)

        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 1)

        user2 = backend.get_user("over2")
        self._override(user_obj, user2)
        response = self.client.get(url, **auth_headers)

        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 0)



    def _override(self, user1, user2):
        user1.override_as = user2
        user1.save()

    def _clear_override(self, user):
        user.override_as = None
        user.save()
