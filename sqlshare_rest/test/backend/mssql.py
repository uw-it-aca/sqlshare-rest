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

    def test_table_from_query(self):
        self.remove_users.append("test_query_save1")
        tmp_db_name = "test_ss_query_db"
        with self.settings(SQLSHARE_QUERY_CACHE_DB=tmp_db_name):
            try:
                backend = get_backend()

                user = backend.get_user("test_query_save1")
                cursor1 = backend.run_query("select (1), ('a1234'), (1), (1.2), (NULL) UNION select (2), ('b'), (4), (NULL), (3)", user, return_cursor=True)

                coldef = backend._get_column_definitions_for_cursor(cursor1)
                self.assertEquals(coldef, "COLUMN1 INT NOT NULL, COLUMN2 VARCHAR(5) NOT NULL, COLUMN3 INT NOT NULL, COLUMN4 FLOAT, COLUMN5 INT")

                backend.create_table_from_query_result("test_query1", cursor1)

                cursor = connection.cursor()
                # See if we have the data in the table!
                db_name = backend.get_query_cache_db_name()
                cursor.execute("SELECT * FROM %s.dbo.test_query1" % db_name)
                data = cursor.fetchall()
                self.assertEquals(data[0][0], 1)
                self.assertEquals(data[0][1], 'a1234')
                self.assertEquals(data[0][2], 1)
                self.assertEquals(data[0][3], 1.2)
                self.assertEquals(data[0][4], None)
                self.assertEquals(data[1][0], 2)
                self.assertEquals(data[1][1], 'b')
                self.assertEquals(data[1][2], 4)
                self.assertEquals(data[1][3], None)
                self.assertEquals(data[1][4], 3)

                cursor.execute("DROP TABLE [%s].dbo.[test_query1]" % db_name)

            except Exception as ex:
                print ("EX: ", ex)
                raise
            finally:
                backend.close_user_connection(user)
                cursor = connection.cursor()
                try:
                    cursor.execute("DROP DATABASE %s" % tmp_db_name)
                except Exception:
                    pass


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

    def test_delete_dataset(self):
        self.remove_users.append("test_user_delete_dataset1")
        backend = get_backend()
        user = backend.get_user("test_user_delete_dataset1")

        handle = StringIO("z,y,x\n1,3,4\n2,10,12")

        parser = Parser()
        parser.guess(handle.read(1024*20))
        handle.seek(0)
        parser.parse(handle)

        try:
            backend.create_dataset_from_parser("soon_to_be_gone", parser, user)
            result = backend.run_query("SELECT * FROM %s.soon_to_be_gone" % user.schema, user)
            self.assertEquals(result[0][0], 1)
            self.assertEquals(result[0][1], 3)
            self.assertEquals(result[0][2], 4)
            self.assertEquals(result[1][0], 2)
            self.assertEquals(result[1][1], 10)
            self.assertEquals(result[1][2], 12)

            backend.delete_dataset("soon_to_be_gone", user)

            import pyodbc
            with self.assertRaises(pyodbc.ProgrammingError):
                backend.run_query("SELECT * FROM %s.soon_to_be_gone" % user.schema, user)

            result = backend.run_query("SELECT * FROM %s.table_soon_to_be_gone" % user.schema, user)
            self.assertEquals(result[0][0], 1)
            self.assertEquals(result[0][1], 3)
            self.assertEquals(result[0][2], 4)
            self.assertEquals(result[1][0], 2)
            self.assertEquals(result[1][1], 10)
            self.assertEquals(result[1][2], 12)

            backend.delete_table("table_soon_to_be_gone", user)
            with self.assertRaises(pyodbc.ProgrammingError):
                backend.run_query("SELECT * FROM %s.table_soon_to_be_gone" % user.schema, user)

        except Exception:
            raise
        finally:
            backend.close_user_connection(user)

    def test_basic_permissions(self):
        try:
            self.remove_users.append("test_user_perm1")
            self.remove_users.append("test_user_perm2")
            backend = get_backend()
            user1 = backend.get_user("test_user_perm1")
            user2 = backend.get_user("test_user_perm2")

            backend.run_query("create table test_user_perm1.test1 (id int)", user1, return_cursor=True).close()
            r2 = backend.run_query("insert into test_user_perm1.test1 (id) values (1)", user1, return_cursor=True).close()

            r3 = backend.run_query("SELECT * from %s.test1" % user1.schema, user1)
            self.assertEquals(len(r3), 1)
            self.assertEquals(r3[0][0], 1)

            backend.close_user_connection(user1)
            import pyodbc
            # User2 doesn't have access to user1!
            with self.assertRaises(pyodbc.ProgrammingError):
                backend.run_query("SELECT * from %s.test1" % user1.schema, user2)

        except Exception as ex:
            print ("EX: ", ex)
            raise
        finally:
            backend.close_user_connection(user1)
            backend.close_user_connection(user2)


    def test_permissions_control(self):
        import pyodbc
        self.remove_users.append("test_user_permissions1")
        self.remove_users.append("test_user_permissions2")
        self.remove_users.append("test_user_permissions3")

        backend = get_backend()

        user1 = backend.get_user("test_user_permissions1")
        user2 = backend.get_user("test_user_permissions2")
        user3 = backend.get_user("test_user_permissions3")

        handle = StringIO("A,B,C\n1,3,4\n2,10,12")

        parser = Parser()
        parser.guess(handle.read(1024*20))
        handle.seek(0)
        parser.parse(handle)

        try:
            backend.create_dataset_from_parser("share_me", parser, user1)

            # Just check that it's there:
            result = backend.run_query("SELECT * FROM test_user_permissions1.share_me", user1)
            self.assertEquals(result[1][2], 12)

            # Not shared yet - no access
            backend.close_user_connection(user1)
            self.assertRaises(pyodbc.ProgrammingError, backend.run_query, "SELECT * FROM test_user_permissions1.share_me", user2)

            # Share it
            backend.close_user_connection(user2)
            backend.add_read_access_to_dataset("share_me", user1, user2)

            backend.close_user_connection(user1)
            # Check the new person has access
            result = backend.run_query("SELECT * FROM test_user_permissions1.share_me", user2)
            self.assertEquals(result[0][0], 1)
            self.assertEquals(result[0][1], 3)
            self.assertEquals(result[0][2], 4)
            self.assertEquals(result[1][0], 2)
            self.assertEquals(result[1][1], 10)
            self.assertEquals(result[1][2], 12)

            backend.close_user_connection(user2)
            # Check that the owner still has access
            result = backend.run_query("SELECT * FROM test_user_permissions1.share_me", user1)
            self.assertEquals(result[0][0], 1)
            self.assertEquals(result[0][1], 3)
            self.assertEquals(result[0][2], 4)
            self.assertEquals(result[1][0], 2)
            self.assertEquals(result[1][1], 10)
            self.assertEquals(result[1][2], 12)

            backend.close_user_connection(user1)
            # Make sure only user2 has access, not user3
            self.assertRaises(pyodbc.ProgrammingError, backend.run_query, "SELECT * FROM test_user_permissions1.share_me", user3)

            # Drop the sharing from user2
            backend.close_user_connection(user3)
            backend.remove_access_to_dataset("share_me", user1, user2)

            # Make sure user2 and user3 don't have access
            backend.close_user_connection(user1)
            self.assertRaises(pyodbc.ProgrammingError, backend.run_query, "SELECT * FROM test_user_permissions1.share_me", user2)
            backend.close_user_connection(user2)
            self.assertRaises(pyodbc.ProgrammingError, backend.run_query, "SELECT * FROM test_user_permissions1.share_me", user3)
            backend.close_user_connection(user3)

            # Make sure the owner does have access
            result = backend.run_query("SELECT * FROM test_user_permissions1.share_me", user1)
            self.assertEquals(result[0][0], 1)
            self.assertEquals(result[0][1], 3)
            self.assertEquals(result[0][2], 4)
            self.assertEquals(result[1][0], 2)
            self.assertEquals(result[1][1], 10)
            self.assertEquals(result[1][2], 12)

        except Exception:
            raise
        finally:
            backend.close_user_connection(user1)
            backend.close_user_connection(user2)
            backend.close_user_connection(user3)


    def test_public_permissions_control(self):
        import pyodbc
        self.remove_users.append("test_user_public_permissions1")
        self.remove_users.append("test_user_public_permissions2")
        self.remove_users.append("test_user_public_permissions3")

        backend = get_backend()

        user1 = backend.get_user("test_user_public_permissions1")
        user2 = backend.get_user("test_user_public_permissions2")
        user3 = backend.get_user("test_user_public_permissions3")

        handle = StringIO("A,B,C\n1,3,4\n2,10,12")

        parser = Parser()
        parser.guess(handle.read(1024*20))
        handle.seek(0)
        parser.parse(handle)

        try:
            backend.create_dataset_from_parser("share_me", parser, user1)

            # Just check that it's there:
            result = backend.run_query("SELECT * FROM test_user_public_permissions1.share_me", user1)
            self.assertEquals(result[1][2], 12)

            # Not shared yet - no access
            backend.close_user_connection(user1)
            self.assertRaises(pyodbc.ProgrammingError, backend.run_query, "SELECT * FROM test_user_public_permissions1.share_me", user2)

            # Share it
            backend.close_user_connection(user2)
            backend.add_public_access("share_me", user1)

            backend.close_user_connection(user1)
            # Check the new person has access
            result = backend.run_query("SELECT * FROM test_user_public_permissions1.share_me", user2)
            self.assertEquals(result[0][0], 1)
            self.assertEquals(result[0][1], 3)
            self.assertEquals(result[0][2], 4)
            self.assertEquals(result[1][0], 2)
            self.assertEquals(result[1][1], 10)
            self.assertEquals(result[1][2], 12)

            backend.close_user_connection(user2)
            # Check that the owner still has access
            result = backend.run_query("SELECT * FROM test_user_public_permissions1.share_me", user1)
            self.assertEquals(result[0][0], 1)
            self.assertEquals(result[0][1], 3)
            self.assertEquals(result[0][2], 4)
            self.assertEquals(result[1][0], 2)
            self.assertEquals(result[1][1], 10)
            self.assertEquals(result[1][2], 12)

            backend.close_user_connection(user1)

            # Check that user3 also has access owner
            result = backend.run_query("SELECT * FROM test_user_public_permissions1.share_me", user3)
            self.assertEquals(result[0][0], 1)
            self.assertEquals(result[0][1], 3)
            self.assertEquals(result[0][2], 4)
            self.assertEquals(result[1][0], 2)
            self.assertEquals(result[1][1], 10)
            self.assertEquals(result[1][2], 12)

            backend.close_user_connection(user3)

            # Drop the public sharing
            backend.remove_public_access("share_me", user1)

            # Make sure user2 and user3 don't have access
            backend.close_user_connection(user1)
            self.assertRaises(pyodbc.ProgrammingError, backend.run_query, "SELECT * FROM test_user_public_permissions1.share_me", user2)
            backend.close_user_connection(user2)
            self.assertRaises(pyodbc.ProgrammingError, backend.run_query, "SELECT * FROM test_user_public_permissions1.share_me", user3)
            backend.close_user_connection(user3)

            # Make sure the owner does have access
            result = backend.run_query("SELECT * FROM test_user_public_permissions1.share_me", user1)
            self.assertEquals(result[0][0], 1)
            self.assertEquals(result[0][1], 3)
            self.assertEquals(result[0][2], 4)
            self.assertEquals(result[1][0], 2)
            self.assertEquals(result[1][1], 10)
            self.assertEquals(result[1][2], 12)

        except Exception:
            raise
        finally:
            backend.close_user_connection(user1)
            backend.close_user_connection(user2)
            backend.close_user_connection(user3)



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
        _run_query("drop login test_user_bad_schema2")
        _run_query("drop login test_user_delete_dataset1")
        _run_query("drop login test_user_tcu1")
        _run_query("drop login test_user_trq1")
        _run_query("drop login test_user_view1")
        _run_query("drop login test_user_view2")
        _run_query("drop login test_query_save1")
        _run_query("drop login test_user_perm1")
        _run_query("drop login test_user_perm2")
        _run_query("drop login test_user_permissions1")
        _run_query("drop login test_user_permissions2")
        _run_query("drop login test_user_permissions3")
        _run_query("drop login test_user_bad_schema1")
        _run_query("drop login test_user_dataset1")

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



