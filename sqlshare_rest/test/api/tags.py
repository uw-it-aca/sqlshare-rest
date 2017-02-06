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

class TagAPITest(BaseAPITest):
    def setUp(self):
        super(TagAPITest, self).setUp()
        # Try to cleanup from any previous test runs...
        self.remove_users = []
        self.client = Client()

    def test_owner_tags(self):
        owner = "tag_owner"
        dataset_name = "super_tagged"

        self.remove_users.append(owner)
        backend = get_backend()
        backend.get_user(owner)

        ds1 = create_dataset_from_query(owner, dataset_name, "SELECT(1)")

        url = reverse("sqlshare_view_dataset_tags", kwargs={ 'owner': owner,
                                                             'name': dataset_name})

        owner_auth_headers = self.get_auth_header_for_username(owner)

        response = self.client.get(url, **owner_auth_headers)
        self.assertEquals(response.status_code, 200)

        self.assertEquals(response.content.decode("utf-8"), "[]")

        data = [
            { "name": owner, "tags": [ "tag1", "tag2" ] }
        ]

        response = self.client.put(url, data=json.dumps(data), **owner_auth_headers)
        self.assertEquals(response.status_code, 200)

        response_data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(response_data, data)

        data = [
            { "name": owner, "tags": [ "tag1" ] }
        ]

        response = self.client.put(url, data=json.dumps(data), **owner_auth_headers)
        self.assertEquals(response.status_code, 200)

        response_data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(response_data, data)

        data = [
            { "name": owner, "tags": [ ] }
        ]

        response = self.client.put(url, data=json.dumps(data), **owner_auth_headers)
        self.assertEquals(response.status_code, 200)

        response_data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(response_data, [])


    def test_multi_user(self):
        owner = "tag_owner2"
        user1 = "tag_user1"
        user2 = "tag_user2"

        backend = get_backend()
        backend.get_user(owner)
        backend.get_user(user1)
        backend.get_user(user2)

        dataset_name = "super_tagged_2"
        self.remove_users.append(owner)
        self.remove_users.append(user1)
        self.remove_users.append(user2)

        ds1 = create_dataset_from_query(owner, dataset_name, "SELECT(1)")

        url = reverse("sqlshare_view_dataset_tags", kwargs={ 'owner': owner,
                                                             'name': dataset_name})

        owner_auth_headers = self.get_auth_header_for_username(owner)
        user1_auth_headers = self.get_auth_header_for_username(user1)
        user2_auth_headers = self.get_auth_header_for_username(user2)

        # Make sure a user can't tag before permission is granted:
        data = [
            { "name": user1, "tags": [ "tag1", "tag2" ] }
        ]

        response = self.client.put(url, data=json.dumps(data), **user1_auth_headers)
        self.assertEquals(response.status_code, 403)

        # Grant access to user1
        permissions_url = reverse("sqlshare_view_dataset_permissions", kwargs={'owner':owner, 'name':dataset_name})
        new_data = { "accounts": [ user1 ] }
        response = self.client.put(permissions_url, data=json.dumps(new_data), **owner_auth_headers)

        # Now make sure user1 can add their tags:
        data = [
            { "name": user1, "tags": [ "tag1", "tag2" ] }
        ]

        response = self.client.put(url, data=json.dumps(data), **user1_auth_headers)
        self.assertEquals(response.status_code, 200)

        response_data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(response_data, data)

        # Now make sure user1 can't update the owner's tags:
        data2 = [
            { "name": owner, "tags": [ "tag3", "tag4" ] },
            { "name": user1, "tags": [ "tag1", "tag2" ] },
        ]

        response = self.client.put(url, data=json.dumps(data2), **user1_auth_headers)
        self.assertEquals(response.status_code, 200)

        response_data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(response_data, data)

        # Make sure the owner can set those same values:
        response = self.client.put(url, data=json.dumps(data2), **owner_auth_headers)
        self.assertEquals(response.status_code, 200)

        response_data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(response_data, data2)

        # Now make sure the owner can't add tags as user1
        data3 = [
            { "name": owner, "tags": [ "tag3", "tag4" ] },
            { "name": user1, "tags": [ "tag1", "tag2", "tag5" ] },
        ]

        response = self.client.put(url, data=json.dumps(data3), **owner_auth_headers)
        self.assertEquals(response.status_code, 200)

        response_data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(response_data, data2)

        # Make sure the owner can remove a tag from user1, and add it to their list
        data4 = [
            { "name": owner, "tags": [ "tag1", "tag3", "tag4" ] },
            { "name": user1, "tags": [ "tag2" ] },
        ]

        response = self.client.put(url, data=json.dumps(data4), **owner_auth_headers)
        self.assertEquals(response.status_code, 200)

        response_data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(response_data, data4)

        # Grant user2 access...
        new_data = { "accounts": [ user1, user2 ] }
        response = self.client.put(permissions_url, data=json.dumps(new_data), **owner_auth_headers)

        # make sure user2 can add a tag, but not remove user1's
        data5 = [
            { "name": owner, "tags": [ "tag1", "tag4" ] },
            { "name": user1, "tags": [ ] },
            { "name": user2, "tags": [ "tag99" ] },
        ]

        response = self.client.put(url, data=json.dumps(data5), **user2_auth_headers)
        self.assertEquals(response.status_code, 200)

        response_data = json.loads(response.content.decode("utf-8"))

        data5_correct = [
            { "name": owner, "tags": [ "tag1", "tag3", "tag4" ] },
            { "name": user1, "tags": [ "tag2" ] },
            { "name": user2, "tags": [ "tag99" ] },
        ]
        self.assertEquals(response_data, data5_correct)

        # Make sure all this data is in the dataset api itself:
        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': owner,
                                                        'name': dataset_name })

        response = self.client.get(url, **owner_auth_headers)
        tags = json.loads(response.content.decode("utf-8"))["tags"]

        self.assertEquals(tags, data5_correct)
