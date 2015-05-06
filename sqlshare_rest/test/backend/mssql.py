from django.test import TestCase

from sqlshare_rest.parser import Parser
from django.db import connection
from sqlshare_rest.util.db import is_mssql, get_backend
import unittest
import six
if six.PY2:
    from StringIO import StringIO
elif six.PY3:
    from io import StringIO


@unittest.skipUnless(is_mssql(), "Only test with mssql")
class TestMSSQLBackend(TestCase):

    def test_create_user(self):
        self.assertEquals(1, 1)
        backend = get_backend()
        self.remove_users.append("test_user_tcu1")
        self.remove_users.append("test_user_tcu1@idp.example.edu")
        user1 = backend.get_user("test_user_tcu1")
        user2 = backend.get_user("test_user_tcu1@idp.example.edu")

    def test_run_query(self):
        backend = get_backend()
        self.remove_users.append("test_user_trq1")
        user = backend.get_user("test_user_trq1")

        result = backend.run_query("select (5)", user)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0][0], 5)
        result = backend.run_query("select ('10') union select ('a')", user)
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0][0], "10")
        self.assertEquals(result[1][0], "a")

    def test_create_view(self):
        backend = get_backend()
        self.remove_users.append("test_user_view1")
        user = backend.get_user("test_user_view1")
        backend.create_view("test_view", "SELECT ('1') UNION SELECT ('a')", user, column_names=["Column1"])

        try:
            result = backend.run_query("SELECT * FROM [test_user_view1].[test_view]", user)

            self.assertEquals(result[0][0], "1")
            self.assertEquals(result[1][0], "a")
        except Exception as ex:
            print ("E: ", ex)


    def test_bad_permissions_view(self):
        backend = get_backend()
        self.remove_users.append("test_user_view2")
        self.remove_users.append("test_user_view3")
        user = backend.get_user("test_user_view2")
        user2 = backend.get_user("test_user_view3")

        import pyodbc

        with self.assertRaises(pyodbc.ProgrammingError):
            cursor = backend.run_query("CREATE VIEW [test_user_view3].[test_view] (Column1) AS SELECT ('1') UNION SELECT ('a')", user, return_cursor=True)


    def test_create_dataset(self):
        self.remove_users.append("test_user_dataset1")
        backend = get_backend()
        user = backend.get_user("test_user_dataset1")

        handle = StringIO("z,y,x\n1,3,4\n2,10,12")

        parser = Parser()
        parser.guess(handle.read(1024*20))
        handle.seek(0)
        parser.parse(handle)

        try:
            backend.create_dataset_from_parser("test_dataset1", parser, user)
            result = backend.run_query("SELECT * FROM [%s].[table_test_dataset1]" % user.schema, user)
            self.assertEquals(result[0][0], 1)
            self.assertEquals(result[0][1], 3)
            self.assertEquals(result[0][2], 4)
            self.assertEquals(result[1][0], 2)
            self.assertEquals(result[1][1], 10)
            self.assertEquals(result[1][2], 12)
            result2 = backend.run_query("SELECT * FROM [%s].[test_dataset1]" % user.schema, user)
            self.assertEquals(result[0][0], 1)
            self.assertEquals(result[0][1], 3)
            self.assertEquals(result[0][2], 4)
            self.assertEquals(result[1][0], 2)
            self.assertEquals(result[1][1], 10)
            self.assertEquals(result[1][2], 12)
        except Exception:
            raise
        finally:
            backend.close_user_connection(user)


    def test_create_table_wrong_schema(self):
        self.remove_users.append("test_user_bad_schema1")
        self.remove_users.append("test_user_bad_schema2")
        backend = get_backend()
        user1 = backend.get_user("test_user_bad_schema1")
        user2 = backend.get_user("test_user_bad_schema2")

        backend.run_query("CREATE TABLE [test_user_bad_schema2].[test_table] (c1 int)", user2, return_cursor=True).close()

        import pyodbc
        with self.assertRaises(pyodbc.ProgrammingError):
            backend.run_query("CREATE TABLE [test_user_bad_schema2].[test_table2] (c1 int)", user1, return_cursor=True).close()

    @classmethod
    def setUpClass(cls):
        def _run_query(sql):
            cursor = connection.cursor()
            try:
                cursor.execute(sql)
            except Exception as ex:
                # Hopefully all of these will fail, so ignore the failures
                pass

        # This is just an embarrassing list of things to cleanup if something fails.
        # It gets added to when something like this blocks one of my test runs...
        _run_query("drop login test_user_tcu1@idp_example_edu")
        _run_query("drop login test_user_tcu1")
        _run_query("drop login test_user_trq1")
        _run_query("drop login test_user_view1")
        _run_query("drop login test_user_view2")

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
