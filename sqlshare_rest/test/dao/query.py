from sqlshare_rest.test import CleanUpTestCase
from django.db import connection
from django.test.utils import override_settings
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.dao.query import create_query
from sqlshare_rest.util.query_queue import process_queue
from sqlshare_rest.models import Query
from django.db import connection
from unittest2 import skipUnless
from sqlshare_rest.util.db import is_pg
import json

@override_settings(SQLSHARE_QUERY_CACHE_DB="test_ss_query_db")
class TestQueryDAO(CleanUpTestCase):
    def setUp(self):
        super(TestQueryDAO, self).setUp()
        self.remove_users = []
        try:
            cursor = connection.cursor()
            cursor.execute("DROP DATABASE test_ss_query_db")
        except Exception as ex:
            pass

    def test_valid_query(self):
        owner = "dao_query_user1"
        self.remove_users.append(owner)

        # Make sure we're not going to be processing a bunch of extra query objects...
        Query.objects.all().delete()

        query = create_query(owner, "SELECT (1)")

        self.assertEquals(query.is_finished, False)
        self.assertEquals(query.has_error, False)

        query = Query.objects.all()[0]
        remove_pk = query.pk
        process_queue()

        q2 = Query.objects.get(pk=query.pk)

        self.assertEquals(q2.is_finished, True)
        self.assertEquals(q2.error, None)
        self.assertEquals(q2.has_error, False)
        self.assertEquals(q2.rows_total, 1)

    @skipUnless(is_pg(), "Only run on Postgres")
    def test_pg_decimal(self):
        owner = "dao_query_decimal"
        self.remove_users.append(owner)
        # Make sure we're not going to be processing a bunch of extra query objects...
        Query.objects.all().delete()

        #query = create_query(owner, "SELECT (1.2 AS DECIMAL)")
        query = create_query(owner, "SELECT CAST(1.2 AS Decimal)")

        self.assertEquals(query.is_finished, False)
        self.assertEquals(query.has_error, False)

        query = Query.objects.all()[0]
        remove_pk = query.pk
        process_queue()

        q2 = Query.objects.get(pk=query.pk)
        data = json.loads(q2.preview_content)
        self.assertEquals(data["data"][0][0], '1.2')


    def test_preview_query(self):
        owner = "dao_query_user1p"
        self.remove_users.append(owner)

        # Make sure we're not going to be processing a bunch of extra query objects...
        Query.objects.all().delete()

        query = create_query(owner, "SELECT (1)", is_preview=True)

        self.assertEquals(query.is_finished, False)
        self.assertEquals(query.has_error, False)

        query = Query.objects.all()[0]
        remove_pk = query.pk
        process_queue()

        q2 = Query.objects.get(pk=query.pk)

        self.assertEquals(q2.is_finished, True)
        self.assertEquals(q2.error, None)
        self.assertEquals(q2.has_error, False)
        self.assertEquals(q2.rows_total, 1)

    def test_order(self):
        owner = "dao_query_user2"
        self.remove_users.append(owner)

        Query.objects.all().delete()

        query1 = create_query(owner, "SELECT (1)")
        query2 = create_query(owner, "SELECT (1)")
        query3 = create_query(owner, "SELECT (1)")

        process_queue()

        q1 = Query.objects.get(pk=query1.pk)
        q2 = Query.objects.get(pk=query2.pk)
        q3 = Query.objects.get(pk=query3.pk)

        self.assertEquals(q1.is_finished, True)
        self.assertEquals(q2.is_finished, False)
        self.assertEquals(q3.is_finished, False)

        process_queue()

        q1 = Query.objects.get(pk=query1.pk)
        q2 = Query.objects.get(pk=query2.pk)
        q3 = Query.objects.get(pk=query3.pk)

        self.assertEquals(q1.is_finished, True)
        self.assertEquals(q2.is_finished, True)
        self.assertEquals(q3.is_finished, False)

        process_queue()
        q1 = Query.objects.get(pk=query1.pk)
        q2 = Query.objects.get(pk=query2.pk)
        q3 = Query.objects.get(pk=query3.pk)

        self.assertEquals(q1.is_finished, True)
        self.assertEquals(q2.is_finished, True)
        self.assertEquals(q3.is_finished, True)

        process_queue()

    def test_invalid_sql(self):
        owner = "dao_query_user3"
        self.remove_users.append(owner)

        Query.objects.all().delete()

        query1 = create_query(owner, "SELECT (1")

        process_queue()

        q1 = Query.objects.get(pk=query1.pk)

        self.assertEquals(q1.is_finished, True)
        self.assertEquals(q1.has_error, True)

        self.assertTrue(q1.error)

    @classmethod
    def setUpClass(cls):
        super(TestQueryDAO, cls).setUpClass()
        def _run_query(sql):
            cursor = connection.cursor()
            try:
                cursor.execute(sql)
            except Exception as ex:
                # Hopefully all of these will fail, so ignore the failures
                pass

        # This is just an embarrassing list of things to cleanup if something fails.
        # It gets added to when something like this blocks one of my test runs...
        _run_query("drop login dao_query_user3")
        _run_query("drop login dao_query_user2")

