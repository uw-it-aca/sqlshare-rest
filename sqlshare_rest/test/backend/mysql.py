from django.test import TestCase
from sqlshare_rest.util.db import is_mysql, get_backend
from sqlshare_rest.parser import Parser
from django.db import connection
from django.conf import settings
from sqlshare_rest.models import Dataset
import unittest
import six
if six.PY2:
    from StringIO import StringIO
elif six.PY3:
    from io import StringIO


@unittest.skipUnless(is_mysql(), "Only test with mysql")
class TestMySQLBackend(TestCase):

    def test_create_user(self):
        self.remove_users.append("test_user_tcu1")
        backend = get_backend()
        user = backend.get_user("test_user_tcu1")

    def test_run_query(self):
        self.remove_users.append("test_user_trq1")
        backend = get_backend()
        user = backend.get_user("test_user_trq1")
        result = backend.run_query("select (5)", user)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0][0], 5)

        result = backend.run_query('select (10) union select ("a")', user)
        backend.close_user_connection(user)
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0][0], '10')
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
                cursor.execute("SELECT * FROM %s.test_query1" % db_name)
                self.assertEquals(cursor.fetchall(), ((1, "a1234", 1, 1.2, None), (2, "b", 4, None, 3)))

                cursor.execute("DROP TABLE `%s`.`test_query1`" % db_name)

            except Exception:
                raise
            finally:
                backend.close_user_connection(user)
                cursor = connection.cursor()
                cursor.execute("DROP DATABASE %s" % tmp_db_name)

    def test_basic_permissions(self):
        from pymysql.err import OperationalError
        try:
            self.remove_users.append("test_user_perm1")
            self.remove_users.append("test_user_perm2")
            backend = get_backend()
            user1 = backend.get_user("test_user_perm1")
            user2 = backend.get_user("test_user_perm2")

            result = backend.run_query("create table test1 (id int primary key auto_increment)", user1)
            r2 = backend.run_query("insert into %s.test1 (id) values (NULL)" % user1.schema, user1)

            r3 = backend.run_query("SELECT * from %s.test1" % user1.schema, user1)
            self.assertEquals(len(r3), 1)
            self.assertEquals(r3[0][0], 1)

            # User2 doesn't have access to user1!
            self.assertRaises(OperationalError, backend.run_query, "SELECT * from %s.test1" % user1.schema, user2)

        except Exception:
            raise
        finally:
            backend.close_user_connection(user1)
            backend.close_user_connection(user2)

    def test_create_view_sql(self):
        backend = get_backend()
        self.assertEquals(backend._create_view_sql("'\";!@#$", "SELECT * from whatever"), "CREATE OR REPLACE VIEW `'\";!@#$` AS SELECT * from whatever")


    def test_create_view(self):
        self.remove_users.append("test_user_view1")
        backend = get_backend()
        user = backend.get_user("test_user_view1")
        backend.create_view("test_view", "SELECT (1) UNION SELECT ('a')", user)

        try:
            result = backend.run_query("SELECT * FROM %s.test_view" % user.schema, user)
            self.assertEquals((('1',),('a',)), result)
            result = backend.run_query("SELECT * FROM test_view", user)
            self.assertEquals((('1',),('a',)), result)
        except Exception:
            raise
        finally:
            backend.close_user_connection(user)

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
            result = backend.run_query("SELECT * FROM %s.table_test_dataset1" % user.schema, user)
            self.assertEquals(((1, 3, 4, ), (2, 10, 12, )), result)
            result2 = backend.run_query("SELECT * FROM %s.test_dataset1" % user.schema, user)
            self.assertEquals(((1, 3, 4, ), (2, 10, 12, )), result2)
        except Exception:
            raise
        finally:
            backend.close_user_connection(user)

    def test_qualified_name(self):
        backend = get_backend()
        owner = "test_mysql_qualified_dataset_name"
        self.remove_users.append(owner)
        backend = get_backend()
        user = backend.get_user(owner)
        dataset = Dataset()
        dataset.owner = user
        dataset.name = "test_table1"

        self.assertEquals(backend.get_qualified_name(dataset), "`test_mysql_qualified_dataset_name`.`test_table1`")

    def test_delete_dataset(self):
        from pymysql.err import ProgrammingError
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
            self.assertEquals(((1, 3, 4, ), (2, 10, 12, )), result)

            backend.delete_dataset("soon_to_be_gone", user)
            self.assertRaises(ProgrammingError, backend.run_query, "SELECT * FROM %s.soon_to_be_gone" % user.schema, user)
            result = backend.run_query("SELECT * FROM %s.table_soon_to_be_gone" % user.schema, user)
            self.assertEquals(((1, 3, 4, ), (2, 10, 12, )), result)
            backend.delete_table("table_soon_to_be_gone", user)
            self.assertRaises(ProgrammingError, backend.run_query, "SELECT * FROM %s.table_soon_to_be_gone" % user.schema, user)

        except Exception:
            raise
        finally:
            backend.close_user_connection(user)

    def test_create_non_square_dataset(self):
        self.remove_users.append("test_user_dataset2")
        backend = get_backend()
        user = backend.get_user("test_user_dataset2")

        handle = StringIO("0,1,2,3,4,5\n0,1,2,3\n0,1")

        parser = Parser()
        parser.guess(handle.read(1024*20))
        handle.seek(0)
        parser.parse(handle)

        try:
            backend.create_dataset_from_parser("test_dataset2", parser, user)
            result = backend.run_query("SELECT * FROM %s.table_test_dataset2" % user.schema, user)
            self.assertEquals(((0, 1, 2, 3, 4, 5, ), (0, 1, 2, 3, None, None, ), (0, 1, None, None, None, None,)), result)
            result2 = backend.run_query("SELECT * FROM %s.test_dataset2" % user.schema, user)
            self.assertEquals(((0, 1, 2, 3, 4, 5, ), (0, 1, 2, 3, None, None, ), (0, 1, None, None, None, None,)), result2)
        except Exception:
            raise
        finally:
            backend.close_user_connection(user)

    def test_non_square_multi_type_dataset(self):
        self.remove_users.append("test_user_dataset3")
        backend = get_backend()
        user = backend.get_user("test_user_dataset3")

        handle = StringIO("0,1.1,a,b\n0,1.2,b\n1")

        parser = Parser()
        parser.guess(handle.read(1024*20))
        handle.seek(0)
        parser.parse(handle)

        try:
            backend.create_dataset_from_parser("test_dataset3", parser, user)
            result = backend.run_query("SELECT * FROM %s.table_test_dataset3" % user.schema, user)
            self.assertEquals(((0, 1.1, "a", "b",), (0, 1.2, "b", None ), (1, None, None, None, )), result)
            result2 = backend.run_query("SELECT * FROM %s.test_dataset3" % user.schema, user)
            self.assertEquals(((0, 1.1, "a", "b",), (0, 1.2, "b", None ), (1, None, None, None, )), result2)
        except Exception:
            raise
        finally:
            backend.close_user_connection(user)

    def test_view_sql_for_dataset(self):
        self.remove_users.append("test_user_tvsfd")
        backend = get_backend()
        user = backend.get_user("test_user_tvsfd")
        sql = backend._get_view_sql_for_dataset("table_';!@$", user)
        self.assertEquals(sql, "SELECT * FROM `%s`.`table_';!@$`" % user.schema)

    def test_load_table_sql(self):
        backend = get_backend()
        sql = backend._load_table_sql("table1';", ["a", 1, 1.112, "';\\%@!!#\n@"])
        self.assertEquals(sql, "INSERT INTO `table1';` VALUES (%s, %s, %s, %s)")

    def test_create_table_sql(self):
        backend = get_backend()
        sql = backend._create_table_sql("test_table1", ["Column1", "Column2", "Column3"], [{ "type": "int" }, { "type": "float" }, { "type": "text", "max": 400 }])

        self.assertEquals(sql, "CREATE TABLE `test_table1` (`Column1` INT, `Column2` FLOAT, `Column3` VARCHAR(400)) ENGINE InnoDB CHARACTER SET utf8 COLLATE utf8_bin")

    def test_snapshot_sql(self):
        backend = get_backend()
        sql = backend._create_snapshot_sql("old", "new")
        self.assertEquals(sql, "CREATE TABLE `new` AS SELECT * FROM old")

    def test_snapshot(self):
        self.remove_users.append("test_user_snapshot1")
        backend = get_backend()
        user = backend.get_user("test_user_snapshot1")

        handle = StringIO("z,y,x\n1,3,4\n2,10,12")

        parser = Parser()
        parser.guess(handle.read(1024*20))
        handle.seek(0)
        parser.parse(handle)

        try:
            backend.create_dataset_from_parser("test_dataset1", parser, user)
            result = backend.run_query("SELECT * FROM %s.table_test_dataset1" % user.schema, user)
            self.assertEquals(((1, 3, 4, ), (2, 10, 12, )), result)
            result2 = backend.run_query("SELECT * FROM %s.test_dataset1" % user.schema, user)
            self.assertEquals(((1, 3, 4, ), (2, 10, 12, )), result2)

            backend.create_snapshot("`test_user_snapshot1`.`test_dataset1`", "test_snapshot1", user)

            # Make sure the snapshot has the right initial data
            result3 = backend.run_query("SELECT * FROM test_user_snapshot1.test_snapshot1", user)
            self.assertEquals(((1, 3, 4, ), (2, 10, 12, )), result3)

            # Update the original backing table
            # make sure the original dataset is updated, but the snapshot isn't
            backend.run_query("INSERT INTO table_test_dataset1 VALUES (3,14,15)", user)
            result4 = backend.run_query("SELECT * FROM %s.test_dataset1" % user.schema, user)
            self.assertEquals(((1, 3, 4, ), (2, 10, 12, ), (3, 14, 15, )), result4)

            result5 = backend.run_query("SELECT * FROM test_user_snapshot1.test_snapshot1", user)
            self.assertEquals(((1, 3, 4, ), (2, 10, 12, )), result5)

        except Exception:
            raise
        finally:
            backend.close_user_connection(user)

    def test_add_access_sql(self):
        self.remove_users.append("test_user_add_access_sql1")
        self.remove_users.append("test_user_add_access_sql2")
        backend = get_backend()
        user1 = backend.get_user("test_user_add_access_sql1")
        user2 = backend.get_user("test_user_add_access_sql2")
        sql = backend._add_read_access_sql("demo_dataset", user1, user2)
        self.assertEquals(sql, "GRANT SELECT ON `test_user_add_access_sql1`.`demo_dataset` TO `meta_dc1031bf6f0`")


    def test_remove_access_sql(self):
        self.remove_users.append("test_user_remove_access_sql1")
        self.remove_users.append("test_user_remove_access_sql2")
        backend = get_backend()
        user1 = backend.get_user("test_user_remove_access_sql1")
        user2 = backend.get_user("test_user_remove_access_sql2")
        sql = backend._remove_read_access_sql("demo_dataset", user1, user2)
        self.assertEquals(sql, "REVOKE ALL PRIVILEGES ON `test_user_remove_access_sql1`.`demo_dataset` FROM `meta_388d357fbb9`")


    def test_permissions_control(self):
        from pymysql.err import OperationalError
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
            # Not shared yet - no access
            self.assertRaises(OperationalError, backend.run_query, "SELECT * FROM `test_user_permissions1`.`share_me`", user2)

            # Share it
            backend.add_read_access_to_dataset("share_me", user1, user2)

            # Check the new person has access
            result = backend.run_query("SELECT * FROM `test_user_permissions1`.`share_me`", user2)
            self.assertEquals(((1, 3, 4, ), (2, 10, 12, )), result)

            # Check that the owner still has access
            result = backend.run_query("SELECT * FROM `test_user_permissions1`.`share_me`", user1)
            self.assertEquals(((1, 3, 4, ), (2, 10, 12, )), result)

            # Make sure only user2 has access, not user3
            self.assertRaises(OperationalError, backend.run_query, "SELECT * FROM `test_user_permissions1`.`share_me`", user3)

            # Drop the sharing from user2
            backend.remove_access_to_dataset("share_me", user1, user2)

            # Make sure user2 and user3 don't have access
            self.assertRaises(OperationalError, backend.run_query, "SELECT * FROM `test_user_permissions1`.`share_me`", user2)
            self.assertRaises(OperationalError, backend.run_query, "SELECT * FROM `test_user_permissions1`.`share_me`", user3)

            # Make sure the owner does have access
            result = backend.run_query("SELECT * FROM `test_user_permissions1`.`share_me`", user1)
            self.assertEquals(((1, 3, 4, ), (2, 10, 12, )), result)

        except Exception:
            raise
        finally:
            backend.close_user_connection(user1)
            backend.close_user_connection(user2)
            backend.close_user_connection(user3)

    def test_dataset_preview_sql(self):
        backend = get_backend()
        self.assertEquals("SELECT * FROM (SELECT (1)) as x LIMIT 100", backend.get_preview_sql_for_query("SELECT (1)"))

    def test_public_datasets(self):
        with self.settings(SQLSHARE_PUBLIC_DB_CONNECTION_USERNAME="ss_test_public",
                           SQLSHARE_PUBLIC_DB_CONNECTION_PASSWORD="fm4i3oj5h",
                           SQLSHARE_PUBLIC_DB_CONNECTION_SCHEMA="ss_test_public_db"):
            cursor = connection.cursor()
            cursor.execute("CREATE USER ss_test_public IDENTIFIED BY 'fm4i3oj5h'")
            cursor.execute("CREATE DATABASE ss_test_public_db")
            cursor.execute("GRANT SELECT ON ss_test_public_db.* to `ss_test_public`")

            from pymysql.err import OperationalError, InternalError
            self.remove_users.append("test_user_public_grant1")
            self.remove_users.append("test_user_public_user")

            backend = get_backend()
            user1 = backend.get_user("test_user_public_grant1")
            user2 = backend.get_user("test_user_public_user")

            handle = StringIO("A,B,C\n1,3,4\n2,10,12")
            parser = Parser()
            parser.guess(handle.read(1024*20))
            handle.seek(0)
            parser.parse(handle)

            handle2 = StringIO("D,E,F\n1,3,4\n2,10,12")

            parser2 = Parser()
            parser2.guess(handle2.read(1024*20))
            handle2.seek(0)
            parser2.parse(handle2)


            try:
                backend.create_dataset_from_parser("share_me", parser, user1)
                backend.create_dataset_from_parser("dont_share_me", parser2, user1)
                # Not shared yet - no access
                self.assertRaises(OperationalError, backend.run_query, "SELECT * FROM `test_user_public_grant1`.`share_me`", user2)

                # Make sure the public query can't access it yet
                self.assertRaises(OperationalError, backend.run_public_query, "SELECT * FROM `test_user_public_grant1`.`share_me`")

                # Make sure some rando can't add public access
                self.assertRaises(InternalError, backend.remove_public_access, "share_me", user2)
                self.assertRaises(OperationalError, backend.run_public_query, "SELECT * FROM `test_user_public_grant1`.`share_me`")

                backend.add_public_access("share_me", user1)

                # Running it as the user will still be an error - can't grant wildcard user access
                self.assertRaises(OperationalError, backend.run_query, "SELECT * FROM `test_user_public_grant1`.`share_me`", user2)

                # But the public query will work
                result = backend.run_public_query("SELECT * FROM `test_user_public_grant1`.`share_me`")
                self.assertEquals(((1, 3, 4, ), (2, 10, 12, )), result)

                # Make sure a query unioning the public with non-public datasets fails
                self.assertRaises(OperationalError, backend.run_public_query, "SELECT * FROM `test_user_public_grant1`.`share_me` LEFT JOIN `test_user_public_grant1`.`dont_share_me` ON A = D")

                # Make sure this query actually works!
                result = backend.run_query("SELECT * FROM `test_user_public_grant1`.`share_me` LEFT JOIN `test_user_public_grant1`.`dont_share_me` ON A = D", user1)
                self.assertEquals(((1, 3, 4, 1, 3, 4,), (2, 10, 12, 2, 10, 12,)), result)

                # Make sure some rando can't remove public access
                self.assertRaises(InternalError, backend.remove_public_access, "share_me", user2)
                result = backend.run_public_query("SELECT * FROM `test_user_public_grant1`.`share_me`")
                self.assertEquals(((1, 3, 4, ), (2, 10, 12, )), result)

                # OK, remove access.
                backend.remove_public_access("share_me", user1)
                self.assertRaises(OperationalError, backend.run_public_query, "SELECT * FROM `test_user_public_grant1`.`share_me`")

                # make sure the owner has access still
                result = backend.run_query("SELECT * FROM `test_user_public_grant1`.`share_me`", user1)
                self.assertEquals(((1, 3, 4, ), (2, 10, 12, )), result)

            except Exception:
                raise
            finally:
                backend.close_user_connection(backend.get_public_user())
                backend.close_user_connection(user1)
                backend.close_user_connection(user2)

                cursor.execute("DROP USER ss_test_public")
                cursor.execute("DROP DATABASE ss_test_public_db")


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
        _run_query("drop user meta_634153bf808")
        _run_query("drop user meta_8daa171745c")
        _run_query("drop user meta_5e19e9d789a")
        _run_query("drop user meta_b26f3aaa573")
        _run_query("drop user meta_b07070ff008")
        _run_query("drop user meta_f762bda7cdc")
        _run_query("drop user meta_c12720852fb")
        _run_query("drop user meta_1a79dccaa61")
        _run_query("drop user meta_e2abbb836ab")
        _run_query("drop user meta_81cfadd5369")
        _run_query("drop user meta_6821430ebab")
        _run_query("drop user meta_2095813758f")
        _run_query("drop user meta_169ef98d749")
        _run_query("drop user meta_be26803663f")
        _run_query("drop user meta_4876e117d6b")
        _run_query("drop user meta_388d357fbb9")
        _run_query("drop user meta_ecbf2a2db0e")
        _run_query("drop user meta_dc1031bf6f0")
        _run_query("drop user meta_a64eaca0830")
        _run_query("drop user meta_3bc4345f6be")
        _run_query("drop user ss_test_public")
        _run_query("drop database test_user_public_user")
        _run_query("drop database ss_test_public_db")
        _run_query("drop database test_user_tcu1")
        _run_query("drop database test_user_trq1")
        _run_query("drop database test_user_tvsfd")
        _run_query("drop database test_user_perm1")
        _run_query("drop database test_user_perm2")
        _run_query("drop database test_user_dataset1")
        _run_query("drop database test_user_snapshot1")
        _run_query("drop database test_user_remove_access_sql1")
        _run_query("drop database test_user_permissions1")
        _run_query("drop database test_user_permissions2")
        _run_query("drop database test_user_permissions3")
        _run_query("drop database test_user_dataset3")
        _run_query("drop database test_user_dataset2")
        _run_query("drop database test_user_dataset1")
        _run_query("drop database test_user_view1")
        _run_query("drop database test_user_add_access_sql1")
        _run_query("drop database test_user_add_access_sql2")
        _run_query("drop database test_user_remove_access_sql2")
        _run_query("drop database test_user_public_grant1")
        _run_query("drop database test_query_save1")

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
