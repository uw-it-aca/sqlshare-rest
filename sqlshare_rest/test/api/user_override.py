from django.core.urlresolvers import reverse
from unittest2 import skipIf
from django.test import RequestFactory
from django.test.utils import override_settings
from django.test.client import Client
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.test.api.base import BaseAPITest
from sqlshare_rest.test import missing_url
from sqlshare_rest.dao.dataset import create_dataset_from_query
from sqlshare_rest.dao.dataset import set_dataset_accounts, set_dataset_emails
from sqlshare_rest.dao.query import create_query
from sqlshare_rest.models import User, Query, DatasetSharingEmail
from sqlshare_rest.dao.user import get_user
from sqlshare_rest.dao.dataset import create_dataset_from_query
import json
import re

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
        super(UserOverrideAPITest, self).setUp()
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
        user = "overrider2"
        self.remove_users.append(user)
        self.remove_users.append("over2")
        user_auth_headers = self.get_auth_header_for_username(user)

        backend = get_backend()
        user_obj = backend.get_user(user)
        self._clear_override(user_obj)

        # Make sure we have the user we think...
        ds_overrider_1 = create_dataset_from_query(user, "ds_overrider_3", "SELECT (1)")
        url = reverse("sqlshare_view_dataset", kwargs={ 'owner': user,
                                                        'name': "ds_overrider_3"})

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
                                                                 'name': "ds_overrider_3"})
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
        user = "overrider_owner_sharer2"
        self.remove_users.append(user)
        self.remove_users.append("overrider_recipient1")
        self.remove_users.append("over2")

        backend = get_backend()
        backend.get_user(user)
        user_obj = backend.get_user("overrider_recipient1")
        auth_headers = self.get_auth_header_for_username("overrider_recipient1")
        self._clear_override(user_obj)

        ds_overrider_1 = create_dataset_from_query(user, "ds_overrider_list2", "SELECT (1)")


        set_dataset_accounts(ds_overrider_1, [ "overrider_recipient1" ])
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

        # Other tests make datasets public, so we can't just count on a static number
        actual_owner_count = len(data)
        self.assertTrue(actual_owner_count >= 1)

        user2 = backend.get_user("over2")
        self._override(user_obj, user2)
        response = self.client.get(url, **auth_headers)

        data = json.loads(response.content.decode("utf-8"))

        # This override user should have 1 fewer than the owner
        self.assertEquals(len(data), actual_owner_count-1)

    def test_dataset_list_tagged(self):
        self.remove_users = []
        user = "overrider_owner_list_tagged"
        self.remove_users.append(user)
        self.remove_users.append("over2")
        auth_headers = self.get_auth_header_for_username(user)

        backend = get_backend()
        user_obj = backend.get_user(user)
        self._clear_override(user_obj)

        ds_overrider_1 = create_dataset_from_query(user, "ds_overrider_list4", "SELECT (1)")

        tag_url = reverse("sqlshare_view_dataset_tags", kwargs={ 'owner': user, 'name': "ds_overrider_list4"})
        data = [ { "name": user, "tags": [ "tag1", "test_override" ] } ]
        self.client.put(tag_url, data=json.dumps(data), **auth_headers)

        url = reverse("sqlshare_view_dataset_tagged_list", kwargs={"tag": "test_override" })
        response = self.client.get(url, **auth_headers)

        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 1)

        user2 = backend.get_user("over2")
        self._override(user_obj, user2)
        response = self.client.get(url, **auth_headers)

        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 0)

    def test_start_query(self):
        owner = "override_query_user1"
        self.remove_users.append(owner)
        self.remove_users.append("user2")

        backend = get_backend()
        user_obj = backend.get_user(owner)
        self._clear_override(user_obj)

        post_url = reverse("sqlshare_view_query_list")
        auth_headers = self.get_auth_header_for_username(owner)

        data = {
            "sql": "select(1)"
        }

        response = self.client.post(post_url, data=json.dumps(data), content_type='application/json', **auth_headers)

        self.assertEquals(response.status_code, 202)

        values = json.loads(response.content.decode("utf-8"))

        self.assertEquals(values["error"], None)
        self.assertEquals(values["sql_code"], "select(1)")
        url = values["url"]

        self.assertTrue(re.match("/v3/db/query/[\d]+$", url))

        response = self.client.get(url, **auth_headers)

        self.assertEquals(response.status_code, 202)
        values = json.loads(response.content.decode("utf-8"))


        user2 = backend.get_user("over2")
        self._override(user_obj, user2)
        response = self.client.get(url, **auth_headers)

        self.assertEquals(response.status_code, 403)

    def test_dataset_tags(self):
        self.remove_users = []
        user = "overrider3"
        self.remove_users.append(user)
        self.remove_users.append("over2")
        user_auth_headers = self.get_auth_header_for_username(user)

        backend = get_backend()
        user_obj = backend.get_user(user)
        self._clear_override(user_obj)

        # Make sure we have the user we think...
        ds_overrider_1 = create_dataset_from_query(user, "ds_overrider_1", "SELECT (1)")
        url = reverse("sqlshare_view_dataset_tags", kwargs={ 'owner': user,
                                                        'name': "ds_overrider_1"})

        response = self.client.get(url, **user_auth_headers)
        self.assertEquals(response.status_code, 200)

        user2 = backend.get_user("over2")
        self._override(user_obj, user2)

        # Now test get as someone else.
        response = self.client.get(url, **user_auth_headers)
        self.assertEquals(response.status_code, 403)

    def test_file_upload_init(self):
        self.remove_users = []
        user = "overrider4"
        self.remove_users.append(user)
        self.remove_users.append("over2")
        auth_headers = self.get_auth_header_for_username(user)

        data1 = "col1,col2,XXcol3\na,1,2\nb,2,3\nc,3,4\n"
        init_url = reverse("sqlshare_view_file_upload_init")

        backend = get_backend()
        user_obj = backend.get_user(user)
        # Do the initial file upload as the other user, make sure actual user
        # can't see the parser values.
        user2 = backend.get_user("over2")
        self._override(user_obj, user2)

        response1 = self.client.post(init_url, data=data1, content_type="text/plain", **auth_headers)
        self.assertEquals(response1.status_code, 201)
        body = response1.content.decode("utf-8")

        re.match("^\d+$", body)

        upload_id = int(body)

        parser_url = reverse("sqlshare_view_file_parser", kwargs={ "id":upload_id })
        response2 = self.client.get(parser_url, **auth_headers)
        self.assertEquals(response2.status_code, 200)

        self._clear_override(user_obj)
        parser_url = reverse("sqlshare_view_file_parser", kwargs={ "id":upload_id })
        response2 = self.client.get(parser_url, **auth_headers)
        self.assertEquals(response2.status_code, 403)

    def test_file_upload_process(self):
        self.remove_users = []
        user = "overrider5"
        self.remove_users.append(user)
        self.remove_users.append("over2")
        auth_headers = self.get_auth_header_for_username(user)

        data1 = "col1,col2,XXcol3\na,1,2\nb,2,3\nc,3,4\n"
        data2 = "z,999,2\ny,2,3\nx,30,41"
        init_url = reverse("sqlshare_view_file_upload_init")

        backend = get_backend()
        user_obj = backend.get_user(user)
        # Do the initial file upload as the other user, make sure actual user
        # can't upload more data.
        user2 = backend.get_user("over2")
        self._override(user_obj, user2)

        response1 = self.client.post(init_url, data=data1, content_type="text/plain", **auth_headers)
        self.assertEquals(response1.status_code, 201)
        body = response1.content.decode("utf-8")

        re.match("^\d+$", body)

        upload_id = int(body)

        parser_url = reverse("sqlshare_view_file_parser", kwargs={ "id":upload_id })
        response2 = self.client.get(parser_url, **auth_headers)
        self.assertEquals(response2.status_code, 200)

        parser_url = reverse("sqlshare_view_file_parser", kwargs={ "id":upload_id })
        response2 = self.client.get(parser_url, **auth_headers)
        self.assertEquals(response2.status_code, 200)

        self._clear_override(user_obj)
        upload_url = reverse("sqlshare_view_file_upload", kwargs={ "id":upload_id })
        # Send the rest of the file:
        response6 = self.client.post(upload_url, data=data2, content_type="application/json", **auth_headers)

        self.assertEquals(response6.status_code, 403)
        self._override(user_obj, user2)
        response6 = self.client.post(upload_url, data=data2, content_type="application/json", **auth_headers)
        self.assertEquals(response6.status_code, 200)

        # Make sure the original user can't finalize the dataset
        self._clear_override(user_obj)
        finalize_url = reverse("sqlshare_view_upload_finalize", kwargs={ "id": upload_id })

        finalize_data = json.dumps({ "dataset_name": "test_dataset1",
                                     "description": "Just a test description"
                                   })
        # Make sure no one else can do it!
        response8 = self.client.post(finalize_url, data=finalize_data, content_type="application/json", **auth_headers)
        self.assertEquals(response8.status_code, 403)

        self._override(user_obj, user2)
        response8 = self.client.post(finalize_url, data=finalize_data, content_type="application/json", **auth_headers)
        self.assertEquals(response8.status_code, 202)


    def test_query_list(self):
        self.remove_users = []
        user = "overrider6"
        self.remove_users.append(user)
        self.remove_users.append("over2")
        auth_headers = self.get_auth_header_for_username(user)

        Query.objects.all().delete()
        backend = get_backend()
        user_obj = backend.get_user(user)
        self._clear_override(user_obj)

        query1 = create_query(user, "SELECT (1)")
        query2 = create_query(user, "SELECT (1)")
        query3 = create_query(user, "SELECT (1)")

        url = reverse("sqlshare_view_query_list")
        auth_headers = self.get_auth_header_for_username(user)

        response = self.client.get(url, **auth_headers)

        self.assertEquals(response.status_code, 200)

        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 3)

        user2 = backend.get_user("over2")
        self._override(user_obj, user2)
        response = self.client.get(url, **auth_headers)

        self.assertEquals(response.status_code, 200)

        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 0)

    def test_query_post(self):
        self.remove_users = []
        user = "overrider7"
        self.remove_users.append(user)
        self.remove_users.append("over2")
        auth_headers = self.get_auth_header_for_username(user)
        url = reverse("sqlshare_view_query_list")

        Query.objects.all().delete()
        backend = get_backend()
        user_obj = backend.get_user(user)
        user2 = backend.get_user("over2")
        self._override(user_obj, user2)

        # make that query as the override user:
        data = {
            "sql": "select(1)"
        }

        response = self.client.post(url, data=json.dumps(data), content_type='application/json', **auth_headers)

        self.assertEquals(response.status_code, 202)


        # find the query as the override...
        response = self.client.get(url, **auth_headers)
        self.assertEquals(response.status_code, 200)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 1)


        self._clear_override(user_obj)

        # make sure the original user can't see the query
        response = self.client.get(url, **auth_headers)
        self.assertEquals(response.status_code, 200)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEquals(len(data), 0)

    def test_dataset_permissions(self):
        self.remove_users = []
        user = "overrider8"
        self.remove_users.append(user)
        self.remove_users.append("over2")
        user_auth_headers = self.get_auth_header_for_username(user)

        backend = get_backend()
        user_obj = backend.get_user(user)
        self._clear_override(user_obj)

        # Make sure we have the user we think...
        ds_overrider_1 = create_dataset_from_query(user, "ds_overrider_2", "SELECT (1)")
        url = reverse("sqlshare_view_dataset_permissions", kwargs={ 'owner': user,
                                                                    'name': "ds_overrider_2"})

        response = self.client.get(url, **user_auth_headers)
        self.assertEquals(response.status_code, 200)

        user2 = backend.get_user("over2")
        self._override(user_obj, user2)

        # Now test get as someone else.
        response = self.client.get(url, **user_auth_headers)
        self.assertEquals(response.status_code, 403)

    def test_access_tokens(self):
        self.remove_users = []
        user = "overrider_owner_sharer1"
        self.remove_users.append(user)
        self.remove_users.append("override_3rd_party")
        self.remove_users.append("overrider_recipient2")
        self.remove_users.append("over3")

        backend = get_backend()
        backend.get_user(user)
        user_obj = backend.get_user("overrider_recipient2")
        auth_headers = self.get_auth_header_for_username("overrider_recipient2")
        self._clear_override(user_obj)

        ds_overrider_1 = create_dataset_from_query("override_3rd_party", "ds_overrider_access_token", "SELECT (1)")

        set_dataset_emails(ds_overrider_1, [ "test_user1@example.com" ])
        ds_overrider_1.is_shared = True
        ds_overrider_1.save()

        sharing = DatasetSharingEmail.objects.filter(dataset=ds_overrider_1)[0]
        email = sharing.email
        access_token1 = sharing.access_token


        user2 = backend.get_user("over3")
        self._override(user_obj, user2)

        # Get the access token url while overriden, and make sure the original
        # user doesn't have access:
        token1_url = reverse("sqlshare_token_access", kwargs={"token": access_token1})
        response = self.client.post(token1_url, data={}, **auth_headers)

        self.assertEquals(response.status_code, 200)

        ds_url = reverse("sqlshare_view_dataset", kwargs={"owner": "override_3rd_party", "name": "ds_overrider_access_token"})

        response = self.client.get(ds_url, **auth_headers)
        self.assertEquals(response.status_code, 200)

        self._clear_override(user_obj)
        response = self.client.get(ds_url, **auth_headers)
        self.assertEquals(response.status_code, 403)

    def _override(self, user1, user2):
        user1.override_as = user2
        user1.save()

    def _clear_override(self, user):
        user.override_as = None
        user.save()
