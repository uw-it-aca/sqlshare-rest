from sqlshare_rest.test import CleanUpTestCase
from sqlshare_rest.models import Dataset, User, FileUpload
from sqlshare_rest.parser import Parser
from sqlshare_rest.util.db import is_sqlite3, get_backend
from sqlshare_rest.dao.dataset import create_dataset_from_query
from sqlshare_rest.util.snapshot_queue import process_snapshot_queue
from django.db.utils import OperationalError
import unittest
import six
if six.PY2:
    from StringIO import StringIO
elif six.PY3:
    from io import StringIO


@unittest.skipUnless(is_sqlite3(), "Only test with sqlite3")
class TestSQLite3Backend(CleanUpTestCase):
    def test_dataset_preview_sql(self):
        backend = get_backend()
        self.assertEquals("SELECT * FROM (SELECT (1)) LIMIT 100", backend.get_preview_sql_for_query("SELECT (1)"))

    def test_qualified_name(self):
        backend = get_backend()
        owner = "test_sqlite_qualified_dataset_name"
        backend = get_backend()
        user = backend.get_user(owner)
        dataset = Dataset()
        dataset.owner = user
        dataset.name = "test_table1"

        self.assertEquals(backend.get_qualified_name(dataset), "test_table1")

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

    def test_table_from_query(self):
        backend = get_backend()
        user = backend.get_user("test_query_save1")
        cursor1 = backend.run_query("select (1), ('a1234'), (1), (1.2), (NULL) UNION select (2), ('b'), (4), (NULL), (3)", user, return_cursor=True)

    def test_create_dataset(self):
        backend = get_backend()
        user = backend.get_user("test_user_dataset1")

        handle = StringIO("z,y,x\n1,3,4\n2,10,12")

        parser = Parser()
        parser.guess(handle.read(1024*20))
        handle.seek(0)
        parser.parse(handle)
        ul = FileUpload.objects.create(owner=user)

        try:
            backend.create_dataset_from_parser("test_dataset1a", parser, ul, user)
            result = backend.run_query("SELECT * FROM table_test_dataset1a", user)
            self.assertEquals([(1, 3, 4, ), (2, 10, 12, )], result)
            result2 = backend.run_query("SELECT * FROM test_dataset1a", user)
            self.assertEquals([(1, 3, 4, ), (2, 10, 12, )], result2)
        except Exception:
            raise
        finally:
            backend.close_user_connection(user)


    def test_create_table_from_parser(self):
        backend = get_backend()
        user = backend.get_user("test_user_dataset2")

        handle = StringIO("z,y,x\n1,3,4\n2,10,12")

        parser = Parser()
        parser.guess(handle.read(1024*20))
        handle.seek(0)
        parser.parse(handle)

        ul = FileUpload.objects.create(owner=user)
        try:
            backend.create_table_from_parser("test_dataset2", parser, ul, user)
            result = backend.run_query("SELECT * FROM table_test_dataset2", user)
            self.assertEquals([(1, 3, 4, ), (2, 10, 12, )], result)
        except Exception:
            raise
        finally:
            backend.close_user_connection(user)

    def test_create_table_from_parser_with_values(self):
        backend = get_backend()
        user = backend.get_user("test_user_dataset2")

        handle = StringIO("col1,col2,XXcol3\na,1,2\nb,2,3\nc,3,4")

        parser = Parser()
        parser.delimiter(",")
        parser.has_header_row(True)
      #  parser.guess(handle.read(1024*20))
        handle.seek(0)
        parser.parse(handle)
        ul = FileUpload.objects.create(owner=user)

        try:
            backend.create_table_from_parser("test_dataset3", parser, ul, user)
            result = backend.run_query("SELECT * FROM table_test_dataset3", user)
            self.assertEquals([('a', 1, 2), ('b', 2, 3), ('c', 3, 4)], result)
        except Exception:
            raise
        finally:
            backend.close_user_connection(user)

    def test_snapshot(self):
        backend = get_backend()
        owner = "test_user_snapshot1"
        user = backend.get_user(owner)


        ds_source = create_dataset_from_query(owner, "s3_snap_source1", "SELECT (1), (2), (4), (8)")
        result2 = backend.run_query("SELECT * FROM s3_snap_source1", user)
        self.assertEquals([(1, 2, 4, 8,)], result2)

        ds_source = Dataset.objects.get(name="s3_snap_source1", owner=user)
        ds_dest = Dataset.objects.create(name="s3_snap_destination", owner=user)
        backend.create_snapshot_dataset(ds_source, ds_dest, user)

        self.assertRaises(OperationalError, backend.run_query, "SELECT * FROM s3_snap_destination", user)

        process_snapshot_queue(verbose=True, run_once=True)

        result4 = backend.run_query("SELECT * FROM table_s3_snap_destination", user)
        self.assertEquals([(1, 2, 4, 8,)], result4)


        result3 = backend.run_query("SELECT * FROM s3_snap_destination", user)
        self.assertEquals([(1, 2, 4, 8,)], result3)
