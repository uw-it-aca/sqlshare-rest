from django.test import TestCase

from sqlshare_rest.util.db import is_sqlite3, get_backend
import unittest

@unittest.skipUnless(is_sqlite3(), "Only test with sqlite3")
class TestSQLite3Backend(TestCase):

    def test_create_user(self):
        backend = get_backend()
        user = backend.get_user("test_user_tcu1")

    def test_run_query(self):
        backend = get_backend()
        user = backend.get_user("test_user_trq1")
        result = backend.run_query("select (5)", user)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0][0], 5)

        result = backend.run_query('select (10) union select ("a")', user)
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0][0], 10)
        self.assertEquals(result[1][0], "a")

    def test_create_view(self):
        backend = get_backend()
        user = backend.get_user("test_user_view1")
        backend.create_view("test_view", "SELECT (1) UNION SELECT ('a')", user)

        try:
            result = backend.run_query("SELECT * FROM test_view", user)
            self.assertEquals([(1,),(u'a',)], result)
        except Exception as ex:
            print ("E: ", ex)

        backend.close_user_connection(user)
