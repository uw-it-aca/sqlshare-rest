from django.test import TestCase
from sqlshare_rest.dao.dataset import create_dataset_from_query

class TestDatasetDAO(TestCase):
    def test_by_query(self):
        model = create_dataset_from_query(username="dao_user1", dataset_name="test1", sql="SELECT (1)")
        self.assertEquals(model.sql, "SELECT (1)")
        self.assertEquals(model.owner.username, "dao_user1")
        self.assertEquals(model.name, "test1")

        self.assertRaises(Exception, create_dataset_from_query, "dao_user1", "test2", "SELECT (")
