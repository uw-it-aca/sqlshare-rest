from sqlshare_rest.backend.base import DBInterface
from django.db import connection


class SQLite3Backend(DBInterface):
    # Eeep... no users in sqlite, without some special compilation options
    def create_db_user(self, username, password):
        return

    # Maybe this could become separate files at some point?
    def create_db_schema(self, db_username, schema_name):
        return

    def run_query(self, sql, username):
        cursor = connection.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
