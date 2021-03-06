from sqlshare_rest.test import CleanUpTestCase
from sqlshare_rest.models import Dataset, Query, FileUpload
from sqlshare_rest.parser import Parser
from sqlshare_rest.dao.dataset import create_dataset_from_query
from sqlshare_rest.util.snapshot_queue import process_snapshot_queue
from django.db import connection
from sqlshare_rest.util.db import is_mssql, is_sql_azure, get_backend
from sqlshare_rest.util.query_queue import process_queue
import unittest
import six
if six.PY2:
    from StringIO import StringIO
elif six.PY3:
    from io import StringIO


@unittest.skipUnless(is_mssql() or is_sql_azure(), "Only test with mssql")
class TestMSSQLBackend(CleanUpTestCase):
    def test_remove_user(self):
        backend = get_backend()
        user1 = backend.get_user("test_remove_user1")
        user2 = backend.get_user("test_remove_user2")

        backend.run_query("SELECT (1)", user1)
        backend.remove_user("test_remove_user1")
        backend.remove_user("test_remove_user2")

        user3 = backend.get_user("test_remove_user3")
        backend.run_query("SELECT (1)", user3)

        backend.remove_user("test_remove_user3")
        backend.close_user_connection(user1)
        backend.close_user_connection(user2)
        backend.close_user_connection(user3)

    def test_thing2(self):
        self.assertEquals(1, 1)

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
                self.assertEquals(coldef, "[COLUMN1] INT NOT NULL, [COLUMN2] NVARCHAR(5) NOT NULL, [COLUMN3] INT NOT NULL, [COLUMN4] FLOAT, [COLUMN5] INT")

                backend.create_table_from_query_result("test_query1", cursor1)

                cursor = connection.cursor()
                # See if we have the data in the table!
                schema_name = backend.get_query_cache_schema_name()
                cursor.execute("SELECT * FROM %s.test_query1" % schema_name)
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

                cursor.execute("DROP TABLE [%s].[test_query1]" % schema_name)

            except Exception as ex:
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
        backend.create_view("test_view", "SELECT ('1') UNION SELECT ('a')", user, column_names=["column1"])

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
            cursor = backend.run_query("CREATE VIEW [test_user_view3].[test_view] (column1) AS SELECT ('1') UNION SELECT ('a')", user, return_cursor=True)


    def test_create_dataset(self):
        self.remove_users.append("test_user_dataset1")
        backend = get_backend()
        user = backend.get_user("test_user_dataset1")

        handle = StringIO("z,y,x\n1,3,4\n2,10,12")

        parser = Parser()
        parser.guess(handle.read(1024*20))
        handle.seek(0)
        parser.parse(handle)
        ul = FileUpload.objects.create(owner=user)

        try:
            backend.create_dataset_from_parser("test_dataset1", parser, ul, user)
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

    def test_bad_data_after_100(self):
        self.remove_users.append("test_bad_over100")
        backend = get_backend()
        user = backend.get_user("test_bad_over100")

        try:
            data = "1,2,3\n" * 100
            data += "a,b,c\n1,2,3"

            parser = Parser()
            handle = StringIO(data)
            parser.guess(handle.read(1024*20))
            handle.seek(0)
            parser.parse(handle)

            ul = FileUpload.objects.create(owner=user)
            backend.create_dataset_from_parser("test_over100", parser, ul, user)
            result = backend.run_query("SELECT * FROM [%s].[table_test_over100]" % user.schema, user)
            self.assertEquals(len(result), 101)

            result = backend.run_query("SELECT * FROM [%s].[untyped_table_test_over100]" % user.schema, user)
            self.assertEquals(len(result), 1)

            result = backend.run_query("SELECT * FROM [%s].[test_over100]" % user.schema, user)
            self.assertEquals(len(result), 102)
            pass
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

    def test_qualified_name(self):
        backend = get_backend()
        owner = "test_mysql_qualified_dataset_name"
        self.remove_users.append(owner)
        backend = get_backend()
        user = backend.get_user(owner)
        dataset = Dataset()
        dataset.owner = user
        dataset.name = "test_table1"

        self.assertEquals(backend.get_qualified_name(dataset), "[test_mysql_qualified_dataset_name].[test_table1]")

    def test_delete_dataset(self):
        self.remove_users.append("test_user_delete_dataset1")
        backend = get_backend()
        user = backend.get_user("test_user_delete_dataset1")

        handle = StringIO("z,y,x\n1,3,4\n2,10,12")

        parser = Parser()
        parser.guess(handle.read(1024*20))
        handle.seek(0)
        parser.parse(handle)
        ul = FileUpload.objects.create(owner=user)

        try:
            backend.create_dataset_from_parser("soon_to_be_gone", parser, ul, user)
            result = backend.run_query("SELECT * FROM %s.soon_to_be_gone" % user.schema, user)
            self.assertEquals(result[0][0], '1')
            self.assertEquals(result[0][1], '3')
            self.assertEquals(result[0][2], '4')
            self.assertEquals(result[1][0], '2')
            self.assertEquals(result[1][1], '10')
            self.assertEquals(result[1][2], '12')

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

            import pyodbc
            # User2 doesn't have access to user1!
            with self.assertRaises(pyodbc.ProgrammingError):
                backend.run_query("SELECT * from %s.test1" % user1.schema, user2)

        except Exception as ex:
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
        ul = FileUpload.objects.create(owner=user1)

        try:
            backend.create_dataset_from_parser("share_me", parser, ul, user1)

            # Just check that it's there:
            result = backend.run_query("SELECT * FROM test_user_permissions1.share_me", user1)
            self.assertEquals(result[1][2], '12')

            # Not shared yet - no access
            self.assertRaises(pyodbc.ProgrammingError, backend.run_query, "SELECT * FROM test_user_permissions1.share_me", user2)

            # Share it
            backend.add_read_access_to_dataset("share_me", user1, user2)

            # Check the new person has access
            result = backend.run_query("SELECT * FROM test_user_permissions1.share_me", user2)
            self.assertEquals(result[0][0], '1')
            self.assertEquals(result[0][1], '3')
            self.assertEquals(result[0][2], '4')
            self.assertEquals(result[1][0], '2')
            self.assertEquals(result[1][1], '10')
            self.assertEquals(result[1][2], '12')

            # Check that the owner still has access
            result = backend.run_query("SELECT * FROM test_user_permissions1.share_me", user1)
            self.assertEquals(result[0][0], '1')
            self.assertEquals(result[0][1], '3')
            self.assertEquals(result[0][2], '4')
            self.assertEquals(result[1][0], '2')
            self.assertEquals(result[1][1], '10')
            self.assertEquals(result[1][2], '12')

            # Make sure only user2 has access, not user3
            self.assertRaises(pyodbc.ProgrammingError, backend.run_query, "SELECT * FROM test_user_permissions1.share_me", user3)

            # Drop the sharing from user2
            backend.remove_access_to_dataset("share_me", user1, user2)

            # Make sure user2 and user3 don't have access
            self.assertRaises(pyodbc.ProgrammingError, backend.run_query, "SELECT * FROM test_user_permissions1.share_me", user2)
            self.assertRaises(pyodbc.ProgrammingError, backend.run_query, "SELECT * FROM test_user_permissions1.share_me", user3)

            # Make sure the owner does have access
            result = backend.run_query("SELECT * FROM test_user_permissions1.share_me", user1)
            self.assertEquals(result[0][0], '1')
            self.assertEquals(result[0][1], '3')
            self.assertEquals(result[0][2], '4')
            self.assertEquals(result[1][0], '2')
            self.assertEquals(result[1][1], '10')
            self.assertEquals(result[1][2], '12')

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
        ul = FileUpload.objects.create(owner=user1)

        try:
            backend.create_dataset_from_parser("share_me", parser, ul, user1)
            dataset = Dataset.objects.create(owner=user1, name="share_me")

            # Just check that it's there:
            result = backend.run_query("SELECT * FROM test_user_public_permissions1.share_me", user1)
            self.assertEquals(result[1][2], '12')

            # Not shared yet - no access
            self.assertRaises(pyodbc.ProgrammingError, backend.run_query, "SELECT * FROM test_user_public_permissions1.share_me", user2)

            # Share it
            backend.add_public_access(dataset, user1)

            # Check the new person has access
            result = backend.run_query("SELECT * FROM test_user_public_permissions1.share_me", user2)
            self.assertEquals(result[0][0], '1')
            self.assertEquals(result[0][1], '3')
            self.assertEquals(result[0][2], '4')
            self.assertEquals(result[1][0], '2')
            self.assertEquals(result[1][1], '10')
            self.assertEquals(result[1][2], '12')

            # Check that the owner still has access
            result = backend.run_query("SELECT * FROM test_user_public_permissions1.share_me", user1)
            self.assertEquals(result[0][0], '1')
            self.assertEquals(result[0][1], '3')
            self.assertEquals(result[0][2], '4')
            self.assertEquals(result[1][0], '2')
            self.assertEquals(result[1][1], '10')
            self.assertEquals(result[1][2], '12')


            # Check that user3 also has access owner
            result = backend.run_query("SELECT * FROM test_user_public_permissions1.share_me", user3)
            self.assertEquals(result[0][0], '1')
            self.assertEquals(result[0][1], '3')
            self.assertEquals(result[0][2], '4')
            self.assertEquals(result[1][0], '2')
            self.assertEquals(result[1][1], '10')
            self.assertEquals(result[1][2], '12')


            # Drop the public sharing
            backend.remove_public_access(dataset, user1)

            # Make sure user2 and user3 don't have access
            self.assertRaises(pyodbc.ProgrammingError, backend.run_query, "SELECT * FROM test_user_public_permissions1.share_me", user2)
            self.assertRaises(pyodbc.ProgrammingError, backend.run_query, "SELECT * FROM test_user_public_permissions1.share_me", user3)

            # Make sure the owner does have access
            result = backend.run_query("SELECT * FROM test_user_public_permissions1.share_me", user1)
            self.assertEquals(result[0][0], '1')
            self.assertEquals(result[0][1], '3')
            self.assertEquals(result[0][2], '4')
            self.assertEquals(result[1][0], '2')
            self.assertEquals(result[1][1], '10')
            self.assertEquals(result[1][2], '12')

        except Exception:
            raise
        finally:
            backend.close_user_connection(user1)
            backend.close_user_connection(user2)
            backend.close_user_connection(user3)

    def test_snapshot(self):
        import pyodbc
        owner = "test_user_snapshot1"
        self.remove_users.append(owner)
        backend = get_backend()
        user = backend.get_user(owner)

        try:
            ds_source = create_dataset_from_query(owner, "my_snap_source1", "SELECT (1), (2), (4), (8)")
            result2 = backend.run_query("SELECT * FROM test_user_snapshot1.my_snap_source1", user)
            self.assertEquals(result2[0][2], 4)

            ds_source = Dataset.objects.get(name="my_snap_source1", owner=user)
            ds_dest = Dataset.objects.create(name="my_snap_destination", owner=user)
            backend.create_snapshot_dataset(ds_source, ds_dest, user)

            self.assertRaises(pyodbc.ProgrammingError, backend.run_query, "SELECT * FROM test_user_snapshot1.my_snap_destination", user)

            process_snapshot_queue(verbose=True)

            result4 = backend.run_query("SELECT * FROM [test_user_snapshot1].[table_my_snap_destination]", user)
            self.assertEquals(result4[0][2], 4)


            result3 = backend.run_query("SELECT * FROM [test_user_snapshot1].[my_snap_destination]", user)
            self.assertEquals(result3[0][2], 4)
        except Exception:
            raise
        finally:
            backend.close_user_connection(user)

    def test_top_rows_query_sql(self):
        backend = get_backend()

        self.assertEquals("SELECT TOP 100 (1), (2)", backend.get_preview_sql_for_query("SELECT (1), (2)"))
        self.assertEquals("SELECT TOP 100 (1), (2)", backend.get_preview_sql_for_query("           SELECT               (1), (2)"))
        self.assertEquals("SELECT TOP 100 (1), (2)", backend.get_preview_sql_for_query("SELECT TOP 400 (1), (2)"))
        self.assertEquals("SELECT TOP 100 (1), (2)", backend.get_preview_sql_for_query("SELECT TOP(400) (1), (2)"))
        self.assertEquals("SELECT TOP 100 (1), (2)", backend.get_preview_sql_for_query("SELECT TOP(20) PERCENT (1), (2)"))
        self.assertEquals("SELECT TOP 100 (1), (2)", backend.get_preview_sql_for_query("select top (20) percent (1), (2)"))
        self.assertEquals("select top (20) (1), (2)", backend.get_preview_sql_for_query("select top (20) (1), (2)"))
        self.assertEquals("select top(20) (1), (2)", backend.get_preview_sql_for_query("select top(20) (1), (2)"))
        self.assertEquals("select top  20  (1), (2)", backend.get_preview_sql_for_query("select top  20  (1), (2)"))
        self.assertEquals("INSERT INTO BLAH...", backend.get_preview_sql_for_query("INSERT INTO BLAH..."))
        self.assertEquals("SELECT TOP 100 (1), (2) UNION SELECT (2), (3)", backend.get_preview_sql_for_query("SELECT (1), (2) UNION SELECT (2), (3)"))

        # TOP and DISTINCT don't play well with each other.
        self.assertEquals("SELECT DISTINCT(10), (1)", backend.get_preview_sql_for_query("SELECT DISTINCT(10), (1)"))

    def test_column_types(self):
        owner = "test_column_types_user"
        self.remove_users.append(owner)
        backend = get_backend()
        user = backend.get_user(owner)

        try:
            backend.run_query("CREATE TABLE [test_column_types_user].[testing_col_types] (c1 bigint, c2 bit, c3 decimal, c4 int, c5 money, c6 numeric, c7 smallint, c8 smallmoney, c9 tinyint, c10 float, c11 real, c12 date, c13 datetime2, c14 datetime, c15 datetimeoffset, c16 smalldatetime, c17 time, c18 char, c19 text, c20 varchar(30), c21 nchar, c22 ntext, c23 nvarchar(30), c24 binary(80), c25 image, c26 varbinary(20), c28 hierarchyid, c31 timestamp, c32 uniqueidentifier, c33 xml, c34 geometry, c35 geography)", user, return_cursor=True).close()

            backend.run_query("INSERT INTO [test_column_types_user].[testing_col_types] VALUES (100, 0, 1.1, 1, 10.12, 123, 1, 1.10, 1, 1.001, 1.1, '2013-01-01', '2013-01-01T13:32:01.0000123', '2013-01-01T23:23:12.004', '2015-06-22T09:47:00Z', '2007-05-08 12:35:00', '19:00:01', 'w', 'sfwewf', 'wfwefwef', 'v', 'wefwefwe', 'wevwevwe', CAST( 123456 AS BINARY(2) ), CAST( 123456 AS BINARY(2) ), CAST( 123456 AS BINARY(2) ), '/0.1/0.2/', DEFAULT, '0E984725-C51C-4BF4-9960-E1C80E27ABA0', '<node></node>', geometry::STGeomFromText('POLYGON ((0 0, 150 0, 150 150, 0 150, 0 0))', 0), geography::STGeomFromText('POLYGON((-122.358 47.653 , -122.348 47.649, -122.348 47.658, -122.358 47.658, -122.358 47.653))', 4326))", user, return_cursor=True).close()


            # Clear out any existing query objects...
            Query.objects.all().delete()
        
            model = create_dataset_from_query(username=owner, dataset_name="test1", sql="SELECT * FROM [test_column_types_user].[testing_col_types]")

            query = Query.objects.all()[0]
            remove_pk = query.pk
            process_queue(verbose=True)

        except Exception as ex:
            print ("E1: ", ex)
        finally:
            try:
                backend.run_query("DROP TABLE [test_column_types_user].[testing_col_types]", user, return_cursor=True).close()
            except Exception:
                pass

    @classmethod
    def setUpClass(cls):
        super(TestMSSQLBackend, cls).setUpClass()
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
        _run_query("drop login test_user_public_permissions1")
        _run_query("drop login test_user_public_permissions2")
        _run_query("drop login test_user_public_permissions3")
        _run_query("drop login test_remove_user2")
        _run_query("drop login test_remove_user1")
        _run_query("drop login test_remove_user3")
        _run_query("drop login test_mysql_qualified_dataset_name")
        _run_query("drop login test_user_snapshot1")
        _run_query("drop login test_column_types_user")

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



