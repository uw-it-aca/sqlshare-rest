from django.test import TestCase
from sqlshare_rest.util.db import is_mysql, get_backend
from sqlshare_rest.parser import Parser
from django.db import connection
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

    @classmethod
    def setUpClass(cls):
        def _run_query(sql):
            cursor = connection.cursor()
            try:
                cursor.execute(sql)
            except Exception as ex:
                # Hopefully all of these will fail, so ignore the failures
                pass

        _run_query("drop user meta_634153bf808")
        _run_query("drop user meta_8daa171745c")
        _run_query("drop user meta_5e19e9d789a")
        _run_query("drop user meta_b26f3aaa573")
        _run_query("drop user meta_b07070ff008")
        _run_query("drop database test_user_tcu1")
        _run_query("drop database test_user_trq1")
        _run_query("drop database test_user_perm2")
        _run_query("drop database test_user_dataset1")

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
