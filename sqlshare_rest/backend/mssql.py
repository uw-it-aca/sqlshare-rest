from sqlshare_rest.backend.base import DBInterface
from sqlshare_rest.models import User
from django import db
from django.db import connection
from django.conf import settings
from contextlib import closing
from decimal import Decimal
import re
import hashlib

# Basic permissions to everything
# grant all on *.* to <user>
# Ability to give new users permission to their database
# grant grant option on *.* to <user>


class MSSQLBackend(DBInterface):
    def remove_user(self, username):
        """
        Overriding this method to force a DB reset.  Seems like a bad sign,
        perhaps connections are being pooled improperly.
        """
        db.close_connection()
        return super(MSSQLBackend, self).remove_user(username)

    def get_user(self, user):
        """
        Overriding this method to force a DB reset.  Seems like a bad sign,
        perhaps connections are being pooled improperly.
        """
        db.close_connection()
        return super(MSSQLBackend, self).get_user(user)

    def create_db_user(self, username, password):
        with closing(connection.cursor()) as cursor:
            sql = "CREATE LOGIN %s WITH PASSWORD = '%s'" % (username, password)
            cursor.execute(sql)
            sql = "CREATE USER %s FROM LOGIN %s" % (username, username)
            cursor.execute(sql)

    def get_db_username(self, user):
        # Periods aren't allowed in MS SQL usernames.
        return re.sub('[.]', '_', user)

    def get_db_schema(self, user):
        # stripped down schema name - prevent quoting issues
        return re.sub('[^a-zA-Z0-9@]', '_', user)

    # Maybe this could become separate files at some point?
    def create_db_schema(self, username, schema):
        with closing(connection.cursor()) as cursor:
            cursor.execute("CREATE SCHEMA %s AUTHORIZATION %s" % (schema,
                                                                  username))
            cursor.execute("GRANT CONNECT TO %s" % (username))

            sql = "GRANT CREATE VIEW, CREATE TABLE TO %s" % (username)
            cursor.execute(sql)

    def remove_db_user(self, user):
        with closing(connection.cursor()) as cursor:
            model = User.objects.get(db_username=user)
            # without this you can't drop the user, because they own an
            # object in the db.
            sql = "ALTER AUTHORIZATION ON SCHEMA::%s TO dbo" % (model.schema)
            cursor.execute(sql)
            # MSSQL doesn't let the username be a placeholder in DROP USER.
            cursor.execute("DROP USER %s" % (user))
            cursor.execute("DROP LOGIN %s" % (user))
        return

    def remove_schema(self, schema):

        pass
        # cursor = connection.cursor()
        # # MySQL doesn't allow placeholders on the db name here.
        # # This is protected by the get_db_schema method, which only allows
        # # a-z, 0-9, and _.
        # schema = self.get_db_schema(schema)
        # cursor.execute("DROP DATABASE %s" % schema)

    def _disconnect_connection(self, connection):
        connection["connection"].close()

    def create_view(self, name, sql, user, column_names=None):
        if column_names:
            columns = ",".join(column_names)
            view_sql = "CREATE VIEW [%s].[%s] (%s) AS %s" % (user.schema,
                                                             name,
                                                             columns,
                                                             sql)
        else:
            view_sql = "CREATE VIEW [%s].[%s] AS %s" % (user.schema, name, sql)
        cursor = self.run_query(view_sql, user, return_cursor=True)
        cursor.close()
        return

    def run_query(self, sql, user, params=None, return_cursor=False):
        connection = self.get_connection_for_user(user)
        cursor = connection.cursor()
        if params:
            # Because, seriously:
            # 'The SQL contains 0 parameter markers,
            # but 1 parameters were supplied'
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        if return_cursor:
            return cursor
        data = cursor.fetchall()
        cursor.close()
        return data

    def _create_user_connection(self, user):
        import pyodbc
        pyodbc.pooling = False

        username = user.db_username
        password = user.db_password
        schema = user.schema

        string = "DSN=%s;UID=%s;PWD=%s;DATABASE=%s;%s" % (
            settings.DATABASES['default']['OPTIONS']['dsn'],
            username,
            password,
            settings.DATABASES['default']['NAME'],
            settings.DATABASES['default']['OPTIONS']['extra_params'],
            )

        from django import db
        db.close_connection()
        return pyodbc.connect(string)

    def _create_table(self, table_name, column_names, column_types, user):
        try:
            sql = self._create_table_sql(user,
                                         table_name,
                                         column_names,
                                         column_types)
            self.run_query(sql, user, return_cursor=True).close()
        except Exception as ex:
            drop_sql = self._drop_exisisting_table_sql(user, table_name)
            self.run_query(drop_sql, user, return_cursor=True).close()
            self.run_query(sql, user, return_cursor=True).close()

    def _drop_exisisting_table_sql(self, user, table_name):
        return "DROP TABLE [%s].[%s]" % (user.schema, table_name)

    def _create_table_sql(self, user, table_name, column_names, column_types):
        def _column_sql(name, col_type):
            if "int" == col_type["type"]:
                return "[%s] int" % name
            if "float" == col_type["type"]:
                return "[%s] float" % name
            if "text" == col_type["type"]:
                return "[%s] varchar(%s)" % (name, col_type["max"])
            # Fallback to text is hopefully good?
            return "[%s] text" % name

        columns = []
        for i in range(0, len(column_names)):
            columns.append(_column_sql(column_names[i], column_types[i]))

        return "CREATE TABLE [%s].[%s] (%s)" % (
                    user.schema,
                    table_name,
                    ", ".join(columns)
               )

    def _load_table_sql(self, table_name, row, user):
        placeholders = map(lambda x: "?", row)
        return "INSERT INTO [%s].[%s] VALUES (%s)" % (user.schema, table_name,
                                                      ", ".join(placeholders))

    def _load_table(self, table_name, data_handle, user):
        for row in data_handle:
            sql = self._load_table_sql(table_name, row, user)
            self.run_query(sql, user, row, return_cursor=True).close()

    def _get_view_sql_for_dataset(self, table_name, user):
        return "SELECT * FROM [%s].[%s]" % (user.schema, table_name)

    def _get_column_definitions_for_cursor(self, cursor):
        import pyodbc
        index = 0
        column_defs = []

        int_type = type(1)
        float_type = type(0.0)
        decimal_type = type(Decimal(0.0))
        str_type = type("")

        for col in cursor.description:
            index = index + 1
            col_type = col[1]
            col_len = col[3]
            null_ok = col[6]

            column_name = "COLUMN%s" % index
            if (col_type == float_type) or (col_type == decimal_type):
                if null_ok:
                    column_defs.append("%s FLOAT" % column_name)
                else:
                    column_defs.append("%s FLOAT NOT NULL" % column_name)
            elif col_type == int_type:
                if null_ok:
                    column_defs.append("%s INT" % column_name)
                else:
                    column_defs.append("%s INT NOT NULL" % column_name)

            elif col_type == str_type and col_len:
                if null_ok:
                    column_defs.append("%s VARCHAR(%s)" % (column_name,
                                                           col_len))
                else:
                    base_str = "%s VARCHAR(%s) NOT NULL"
                    column_defs.append(base_str % (column_name, col_len))
            else:
                column_defs.append("%s TEXT" % column_name)

        return ", ".join(column_defs)

    def create_table_from_query_result(self, name, source_cursor):
        # Make sure the db exists to stash query results into
        QUERY_SCHEMA = self.get_query_cache_db_name()
        cursor = connection.cursor()
        sql = "SELECT name FROM master.sys.databases WHERE name = ?"
        cursor.execute(sql, (QUERY_SCHEMA, ))

        if not cursor.rowcount:
            cursor.execute("CREATE DATABASE %s" % (QUERY_SCHEMA))

        column_def = self._get_column_definitions_for_cursor(source_cursor)

        full_name = "[%s].dbo.[%s]" % (QUERY_SCHEMA, name)
        create_table = "CREATE TABLE %s (%s)" % (full_name, column_def)
        cursor.execute(create_table)

        row = source_cursor.fetchone()

        placeholders = ", ".join(list(map(lambda x: "?", row)))
        insert = "INSERT INTO %s VALUES (%s)" % (full_name, placeholders)
        row_count = 0
        while row:
            cursor.execute(insert, row)
            row = source_cursor.fetchone()
            row_count += 1
        return row_count

    def get_query_cache_db_name(self):
        return getattr(settings, "SQLSHARE_QUERY_CACHE_DB", "ss_query_db")

    def delete_table(self, table, owner):
        sql = "DROP TABLE [%s].[%s]" % (owner.schema, table)
        self.run_query(sql, owner, return_cursor=True).close()

    def delete_dataset(self, dataset_name, owner):
        sql = "DROP VIEW [%s].[%s]" % (owner.schema, dataset_name)
        self.run_query(sql, owner, return_cursor=True).close()
