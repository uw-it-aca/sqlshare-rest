from sqlshare_rest.test import CleanUpTestCase
from sqlshare_rest.util.db import get_backend
from django.db import connection
from sqlshare_rest.dao.dataset import create_dataset_from_query
from django.test.utils import override_settings
from sqlshare_rest.util.query_queue import process_queue
from sqlshare_rest.models import Query, Dataset
from django.db import transaction

@override_settings(SQLSHARE_QUERY_CACHE_DB="test_ss_query_db", DEBUG=True)
class TestDatasetDAO(CleanUpTestCase):
    def test_by_query(self):
        self.remove_users.append("dao_user1")
        model = create_dataset_from_query(username="dao_user1", dataset_name="test1", sql="SELECT (1)")
        self.assertEquals(model.sql, "SELECT (1)")
        self.assertEquals(model.owner.username, "dao_user1")
        self.assertEquals(model.name, "test1")

        self.assertRaises(Exception, create_dataset_from_query, "dao_user1", "test2", "SELECT (")

    def test_preview(self):
        owner = "dataset_dao_user2"
        self.remove_users.append(owner)

        Query.objects.all().delete()

        model = create_dataset_from_query(username=owner, dataset_name="test3", sql="SELECT (3)")

        self.assertEquals(model.get_sample_data_status(), "working")

        query = Query.objects.all()[0]
        remove_pk = query.pk
        process_queue()

        m2 = Dataset.objects.get(pk=model.pk)

        self.assertEquals(m2.get_sample_data_status(), "success")

    def setUp(self):
        # Try to cleanup from any previous test runs...
        self.remove_users = []

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
        _run_query("drop login xcmd_line_test2")
        _run_query("drop login xcmd_line_test1")
        _run_query("drop user xcmd_line_test2")
        _run_query("drop user xcmd_line_test1")
        _run_query("drop login dataset_dao_user2")
        _run_query("drop user dataset_dao_user2")
        _run_query("drop login dao_user1")
        _run_query("drop user dao_user1")
        _run_query("drop user meta_ce52aaec1a0")
        _run_query("drop database dao_user1")
