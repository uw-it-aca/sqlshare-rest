from sqlshare_rest.backend.base import DBInterface
from django.db import connection


class SQLite3Backend(DBInterface):
    # Eeep... no users in sqlite, without some special compilation options
    def create_db_user(self, username, password):
        return

    # This is here so some amount of the permissions api unit tests
    # can be run w/ sqlite3.
    def add_read_access_to_dataset(*args, **kwargs):
        pass

    def remove_access_to_dataset(*args, **kwargs):
        pass

    def create_view(self, name, sql, user):
        view_sql = "CREATE VIEW %s AS %s" % (name, sql)
        self.run_query(view_sql, user)
        return

    # Maybe this could become separate files at some point?
    def create_db_schema(self, db_username, schema_name):
        return

    def run_query(self, sql, username):
        cursor = connection.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
