from django.test import TestCase
from sqlshare_rest.util.db import is_mysql, get_backend
from django.db import connection
import unittest

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

        except Exception as ex:
            print ("Err: ", ex)

        backend.close_user_connection(user1)
        backend.close_user_connection(user2)

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
        except Exception as ex:
            print ("E: ", ex)
        backend.close_user_connection(user)

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
        _run_query("drop database test_user_tcu1")
        _run_query("drop database test_user_trq1")
        _run_query("drop database test_user_perm2")

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
