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
from sqlshare_rest.util.dataset_queue import process_dataset_queue
from sqlshare_rest.util.query_queue import process_queue
from sqlshare_rest.models import FileUpload, Query
from sqlshare_rest.util.db import is_mysql, is_sqlite3

@override_settings(SQLSHARE_QUERY_CACHE_DB="test_ss_query_db")
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
        # Clean up any other file uplaods, so the queue processing at the bottom finds ours
        FileUpload.objects.all().delete()

        # We also need to clear up Queries, so we can get the data preview
        Query.objects.all().delete()
        owner = "upload_user1"
        other = "upload_faker1"
        self.remove_users.append(owner)
        self.remove_users.append(other)
        auth_headers = self.get_auth_header_for_username(owner)
        other_auth_headers = self.get_auth_header_for_username(other)

        data1 = "col1,col2,XXcol3\na,1,2\nb,2,3\nc,3,4\n"
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
        self.assertEquals(parser_data["parser"]["has_column_headers"], True)
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
        self.assertEquals(parser_data["parser"]["has_column_headers"], False)
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
        process_dataset_queue()

        response11 = self.client.get(finalize_url, **auth_headers)
        self.assertEquals(response11.status_code, 201)

        dataset_url = response11["Location"]
        self.assertEquals(dataset_url, "http://testserver/v3/db/dataset/upload_user1/test_dataset1")

        process_queue()
        response12 = self.client.get(dataset_url, **auth_headers)

        self.assertEquals(response12.status_code, 200)

        data = json.loads(response12.content.decode("utf-8"))

        if is_sqlite3():
            self.assertEquals(data["sample_data"], [[u"a", u"1", u"2"], [u"b", u"2", u"3"], [u"c", u"3", u"4"], [u"z", u"999", u"2"],[u"y", u"2", u"3"],[u"x", u"30", u"41"],])
        else:
            # Hoping that other db engines will also return typed data...
            self.assertEquals(data["sample_data"], [[u"a", 1, 2], [u"b", 2, 3], [u"c", 3, 4], [u"z", 999, 2],[u"y", 2, 3],[u"x", 30, 41],])

