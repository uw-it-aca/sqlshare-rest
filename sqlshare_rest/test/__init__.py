from django.test import TestCase
from django.core.urlresolvers import reverse
from django.test import TestCase
from sqlshare_rest.util.db import get_backend


def missing_url(name):
    try:
        url = reverse(name)
    except Exception as ex:
        print ("Ex: ", ex)
        return True

    return False

class CleanUpTestCase(TestCase):
    def setUp(self):
        # Try to cleanup from any previous test runs...
        self.remove_users = []

    def tearDown(self):
        backend = get_backend()

        for user in self.remove_users:
            try:
                backend.remove_user(user)
            except Exception as ex:
                print ("Error deleting user: ", ex)

        self.remove_users = []
