from django.test import TestCase
from unittest2 import skipIf
from sqlshare_rest.test import missing_url
from django.test.client import Client
from django.core.urlresolvers import reverse

class DatsetAPITest(TestCase):
    def setUp(self):
        self.client = Client()

    @skipIf(missing_url("sqlshare_view_dataset_list"), "SQLShare REST URLs not configured")
    def test_methods(self):
        url = reverse("sqlshare_view_dataset_list")
        response = self.client.get(url)
        self.assertEquals(response.status_code, 200)
