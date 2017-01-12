from django.core.urlresolvers import reverse
from django.test import SimpleTestCase
from sqlshare_rest.util.db import get_backend


def missing_url(name):
    try:
        url = reverse(name)
    except Exception as ex:
        print ("Ex: ", ex)
        return True

    return False

class CleanUpTestCase(SimpleTestCase):
    @classmethod
    def setUpClass(cls):
        cls.allow_database_queries = True

    def setUp(self):
        # Try to cleanup from any previous test runs...
        self.remove_users = []
        super(CleanUpTestCase, self).setUp()

    def tearDown(self):
        backend = get_backend()

        for user in self.remove_users:
            try:
                backend.remove_user(user)
            except Exception as ex:
                # This is going to fail on at least sqlite3
                pass

        self.remove_users = []
