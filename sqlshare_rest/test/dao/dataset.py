from sqlshare_rest.test import CleanUpTestCase
from django.db import connection
from sqlshare_rest.dao.dataset import create_dataset_from_query

class TestDatasetDAO(CleanUpTestCase):
    def test_by_query(self):
        model = create_dataset_from_query(username="dao_user1", dataset_name="test1", sql="SELECT (1)")
        self.assertEquals(model.sql, "SELECT (1)")
        self.assertEquals(model.owner.username, "dao_user1")
        self.assertEquals(model.name, "test1")

        self.assertRaises(Exception, create_dataset_from_query, "dao_user1", "test2", "SELECT (")

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
        _run_query("drop user meta_ce52aaec1a0")
