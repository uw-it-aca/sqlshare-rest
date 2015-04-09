from django.test import TestCase
from unittest2 import skipIf
from django.db import connection
import json
import re
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
class FileUploadAPITest(BaseAPITest):
    def setUp(self):
        # Try to cleanup from any previous test runs...
        self.remove_users = []
        self.client = Client()

    def test_full_upload(self):
        owner = "upload_user1"
        other = "upload_faker1"
        self.remove_users.append(owner)
        self.remove_users.append(other)
        auth_headers = self.get_auth_header_for_username(owner)
        other_auth_headers = self.get_auth_header_for_username(other)

        data1 = "col1,col2,XXcol3\na,1,2\nb,2,3\nc,3,4"
        data2 = "z,999,2\ny,2,3\nx,30,41"

        init_url = reverse("sqlshare_view_file_upload_init")

        response1 = self.client.post(init_url, data=data1, content_type="text/plain", **auth_headers)
        self.assertEquals(response1.status_code, 201)
        body = response1.content.decode("utf-8")

        re.match("^\d+$", body)

        upload_id = int(body)

        # Test default parsing
        parser_url = reverse("sqlshare_view_file_parser", kwargs={ "id":upload_id })
        response2 = self.client.get(parser_url, **auth_headers)
        self.assertEquals(response2.status_code, 200)

        parser_data = json.loads(response2.content.decode("utf-8"))
        self.assertEquals(parser_data["parser"]["delimiter"], ",")
        self.assertEquals(parser_data["parser"]["has_column_header"], True)
        self.assertEquals(parser_data["columns"][0], { "name": "col1" })
        self.assertEquals(parser_data["columns"][1], { "name": "col2" })
        self.assertEquals(parser_data["columns"][2], { "name": "XXcol3" })
        self.assertEquals(parser_data["sample_data"][0], ["a", "1", "2"])
        self.assertEquals(parser_data["sample_data"][1], ["b", "2", "3"])
        self.assertEquals(parser_data["sample_data"][2], ["c", "3", "4"])

        # Test that no one else can access the upload
        response3 = self.client.get(parser_url, **other_auth_headers)
        self.assertEquals(response3.status_code, 403)

        # Test overriding the parser
        response4 = self.client.put(parser_url, data='{ "parser": { "delimiter": "|", "has_column_header": false } }', content_type="application/json", **auth_headers)

        parser_url = reverse("sqlshare_view_file_parser", kwargs={ "id":upload_id })
        response5 = self.client.get(parser_url, **auth_headers)
        self.assertEquals(response2.status_code, 200)

        parser_data = json.loads(response5.content.decode("utf-8"))
        self.assertEquals(parser_data["parser"]["delimiter"], "|")
        self.assertEquals(parser_data["parser"]["has_column_header"], False)
        self.assertEquals(parser_data["columns"], None)
        self.assertEquals(parser_data["sample_data"][0], ["col1,col2,XXcol3"])
        self.assertEquals(parser_data["sample_data"][1], ["a,1,2"])
        self.assertEquals(parser_data["sample_data"][2], ["b,2,3"])
        self.assertEquals(parser_data["sample_data"][3], ["c,3,4"])

        # Set the parser back...
        response5 = self.client.put(parser_url, data='{ "parser": { "delimiter": ",", "has_column_header": true} }', content_type="application/json", **auth_headers)

        upload_url = reverse("sqlshare_view_file_upload", kwargs={ "id":upload_id })
        # Send the rest of the file:
        response6 = self.client.post(upload_url, data=data2, content_type="application/json", **auth_headers)

        self.assertEquals(response6.status_code, 200)

        # Make sure no one else can add content to our upload
        response7 = self.client.post(upload_url, data=data2, content_type="application/json", **other_auth_headers)
        self.assertEquals(response7.status_code, 403)

        # Finalize the upload - turn it into a dataset
        finalize_url = reverse("sqlshare_view_upload_finalize", kwargs={ "id": upload_id })

        finalize_data = json.dumps({ "dataset_name": "test_dataset1",
                                     "description": "Just a test description"
                                   })
        # Make sure no one else can do it!
        response8 = self.client.post(finalize_url, data=finalize_data, content_type="application/json", **other_auth_headers)
        self.assertEquals(response8.status_code, 403)

        response9 = self.client.post(finalize_url, data=finalize_data, content_type="application/json", **auth_headers)
        self.assertEquals(response9.status_code, 202)

        response10 = self.client.get(finalize_url, **auth_headers)
        self.assertEquals(response10.status_code, 202)

        # Process the dataset...
