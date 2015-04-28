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

    def add_public_access(*args, **kwargs):
        pass

    def remove_public_access(*args, **kwargs):
        pass

    def delete_dataset(self, dataset_name, owner):
        sql = "DROP VIEW `%s`" % (dataset_name)
        self.run_query(sql, owner)

    def create_view(self, name, sql, user):
        view_sql = "CREATE VIEW %s AS %s" % (name, sql)
        self.run_query(view_sql, user)
        return

    def get_download_sql_for_dataset(self, dataset):
        return "SELECT * FROM %s" % self.get_qualified_name(dataset)

    def get_preview_sql_for_query(self, sql):
        return "SELECT * FROM (%s) LIMIT 100" % sql

    def get_qualified_name(self, dataset):
        return dataset.name

    # Maybe this could become separate files at some point?
    def create_db_schema(self, db_username, schema_name):
        return

    def run_query(self, sql, username, params=None, return_cursor=False):
        cursor = connection.cursor()
        cursor.execute(sql, params)
        if return_cursor:
            return cursor
        return cursor.fetchall()

    def get_query_sample_sql(self, query_id):
        return "SELECT * FROM query_%s LIMIT 100" % (query_id)

    def create_table_from_query_result(self, name, source_cursor):
        cursor = connection.cursor()

        column_def = self._get_column_definitions_for_cursor(source_cursor)

        create_table = "CREATE TABLE %s (%s)" % (name, column_def)
        cursor.execute(create_table)

        row = source_cursor.fetchone()

        placeholders = ", ".join(list(map(lambda x: "%s", row)))
        insert = "INSERT INTO %s VALUES (%s)" % (name, placeholders)
        row_count = 0
        while row:
            cursor.execute(insert, row)
            row = source_cursor.fetchone()
            row_count += 1
        return row_count

    def add_read_access_to_query(*args, **kwargs):
        pass

    def delete_query(self, query_id):
        sql = "DROP TABLE query_%s" % (query_id)
        cursor = connection.cursor()
        cursor.execute(sql)

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

    def _create_table(self, table_name, column_names, column_types, user):
        sql = self._create_table_sql(table_name, column_names, column_types)
        self.run_query(sql, user)

    def _create_table_sql(self, table_name, column_names, column_types):
        def _column_sql(name, col_type):
            if "int" == col_type["type"]:
                return "`%s` INT" % name
            if "float" == col_type["type"]:
                return "`%s` FLOAT" % name
            if "text" == col_type["type"]:
                return "`%s` VARCHAR(%s)" % (name, col_type["max"])
            # Fallback to text is hopefully good?
            return "`%s` TEXT" % name

        columns = []
        for i in range(0, len(column_names)):
            columns.append(_column_sql(column_names[i], column_types[i]))

        return "CREATE TABLE `%s` (%s)" % (table_name, ", ".join(columns))

    def _load_table_sql(self, table_name, row):
        placeholders = map(lambda x: "?", row)
        return "INSERT INTO `%s` VALUES (%s)" % (table_name,
                                                 ", ".join(placeholders))

    def _load_table(self, table_name, data_handle, user):
        for row in data_handle:
            sql = self._load_table_sql(table_name, row)
            self.run_query(sql, user, row)

    def _get_view_sql_for_dataset(self, table_name, user):
        return "SELECT * FROM `%s`" % (table_name)
