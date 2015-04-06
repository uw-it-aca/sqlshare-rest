from sqlshare_rest.test import CleanUpTestCase
from django.db import connection
from sqlshare_rest.dao.query import create_query
from sqlshare_rest.util.query_queue import process_queue
from sqlshare_rest.models import Query

class TestQueryDAO(CleanUpTestCase):
    def test_valid_query(self):
        owner = "dao_query_user1"
        self.remove_users.append(owner)

        # Make sure we're not going to be processing a bunch of extra query objects...
        Query.objects.all().delete()

        query = create_query(owner, "SELECT (1)")

        self.assertEquals(query.is_finished, False)
        self.assertEquals(query.has_error, False)

        process_queue()

        q2 = Query.objects.get(pk=query.pk)

        self.assertEquals(q2.is_finished, True)
        self.assertEquals(q2.has_error, False)

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

