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

    def get_preview_sql_for_query(self, sql):
        return "SELECT * FROM (%s) LIMIT 100" % sql

    def get_qualified_name(self, dataset):
        return dataset.name

    # Maybe this could become separate files at some point?
    def create_db_schema(self, db_username, schema_name):
        return

    def run_query(self, sql, username, params=None, return_cursor=False):
        cursor = connection.cursor()
        cursor.execute(sql)
        if return_cursor:
            return cursor
        return cursor.fetchall()

    def create_table_from_query_result(self, name, source_cursor):
        cursor = connection.cursor()

        column_def = self._get_column_definitions_for_cursor(source_cursor)

        create_table = "CREATE TABLE %s (%s)" % (name, column_def)
        cursor.execute(create_table)

        row = source_cursor.fetchone()

        placeholders = ", ".join(list(map(lambda x: "%s", row)))
        insert = "INSERT INTO %s VALUES (%s)" % (name, placeholders)
        while row:
            cursor.execute(insert, row)
            row = source_cursor.fetchone()

    def add_read_access_to_query(*args, **kwargs):
        pass

    def _get_column_definitions_for_cursor(self, cursor):
        index = 0
        column_defs = []
        for col in cursor.description:
            index = index + 1
            col_type = col[1]
            col_len = col[3]
            null_ok = col[6]

            # We don't get type info from the query cursors, so just
            # dump everything into text?
            column_name = "COLUMN%s" % index
            column_defs.append("%s TEXT" % column_name)

        return ", ".join(column_defs)
