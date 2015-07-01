from sqlshare_rest.backend.base import DBInterface
from sqlshare_rest.models import User, Query
from django import db
from django.db import connection
from django.conf import settings
from contextlib import closing
from decimal import Decimal
import datetime
import string
import random
import re
import hashlib

# Basic permissions to everything
# grant all on *.* to <user>
# Ability to give new users permission to their database
# grant grant option on *.* to <user>


class MSSQLBackend(DBInterface):
    COLUMN_MAX_LENGTH = 2147483647
    MAX_PARAMETERS = 2099

    def get_user(self, user):
        """
        Overriding this method to force a DB reset.  Seems like a bad sign,
        perhaps connections are being pooled improperly.
        """
#        db.close_connection()
        return super(MSSQLBackend, self).get_user(user)

    def create_db_user_password(self):
        # Added complexity for windows security rules...
        # make sure we have upper and lower case letters, digits and
        # other chars.  then add another 40 of randomly selected chars.
        lower = string.ascii_lowercase
        upper = string.ascii_uppercase
        digits = string.digits
        others = "_-*&^$#!@"
        base = ''.join(random.choice(lower) for i in range(2))
        base += ''.join(random.choice(upper) for i in range(2))
        base += ''.join(random.choice(digits) for i in range(2))
        base += ''.join(random.choice(others) for i in range(2))

        chars = string.ascii_letters + string.digits + others
        password = base + ''.join(random.choice(chars) for i in range(40))
        return password

    def _run_create_db_user(self, conn, username, password):
        cursor = conn.cursor()
        sql = "CREATE LOGIN %s WITH PASSWORD = '%s'" % (username, password)
        cursor.execute(sql)
        cursor.close()
        cursor = connection.cursor()
        sql = "CREATE USER %s FROM LOGIN %s" % (username, username)
        cursor.execute(sql)
        cursor.close()

    def create_db_user(self, username, password):
        return self._run_create_db_user(connection, username, password)

    def get_db_username(self, user):
        # Periods aren't allowed in MS SQL usernames.
        return re.sub('[.]', '_', user)

    def get_db_schema(self, user):
        # stripped down schema name - prevent quoting issues
        return re.sub('[^a-zA-Z0-9@]', '_', user)

    # Maybe this could become separate files at some point?
    def create_db_schema(self, username, schema):
        cursor = connection.cursor()
        cursor.execute("CREATE SCHEMA %s AUTHORIZATION %s" % (schema,
                                                              username))
        cursor.close()
        cursor = connection.cursor()
        cursor.execute("GRANT CONNECT TO %s" % (username))
        cursor.close()

        cursor = connection.cursor()
        sql = "GRANT CREATE VIEW, CREATE TABLE TO %s" % (username)
        cursor.execute(sql)
        cursor.close()
        sql = ("GRANT SELECT ON SCHEMA::%s "
               "TO %s WITH GRANT OPTION" % (schema, username))
        cursor = connection.cursor()
        cursor.execute(sql)
        cursor.close()

    def remove_db_user(self, user):
        model = User.objects.get(db_username=user)
        cursor = connection.cursor()
        # without this you can't drop the user, because they own an
        # object in the db.
        sql = "ALTER AUTHORIZATION ON SCHEMA::%s TO dbo" % (model.schema)
        cursor.execute(sql)
        cursor.close()
        # MSSQL doesn't let the username be a placeholder in DROP USER.
        cursor = connection.cursor()
        cursor.execute("DROP USER %s" % (user))
        cursor.close()
        cursor = connection.cursor()
        cursor.execute("DROP LOGIN %s" % (user))
        cursor.close()
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

    def _create_snapshot_table(self, source_dataset, table_name, user):
        source_schema = source_dataset.owner.schema
        sql = "SELECT * INTO [%s].[%s] FROM [%s].[%s]" % (source_schema,
                                                          table_name,
                                                          user.schema,
                                                          source_dataset.name)

        self.run_query(sql, user, return_cursor=True).close()

    def _create_view_of_snapshot(self, dataset, user):
        sql = self._get_snapshot_view_sql(dataset)

        view_name = self._get_table_name_for_dataset(dataset.name)
        self._create_placeholder_view(view_name, user)
        self.run_query(sql, user, return_cursor=True).close()

    def _get_snapshot_view_sql(self, dataset):
        table_name = self._get_table_name_for_dataset(dataset.name)
        return ("CREATE VIEW [%s].[%s] AS "
                "SELECT * FROM [%s].[%s]" % (dataset.owner.schema,
                                             dataset.name,
                                             dataset.owner.schema,
                                             table_name))

    def _create_placeholder_view(self, name, user):
        # Create a dummy view - if it fails, no problem, since the create views
        # are now all alter views.
        schema = user.schema
        sql = "CREATE VIEW [%s].[%s] AS SELECT (NULL) as a" % (schema, name)
        try:
            self.run_query(sql, user, return_cursor=True).close()
        except Exception as ex:
            pass

        return

    def create_view(self, name, sql, user, column_names=None):
        import pyodbc
        if column_names:
            columns = ",".join(column_names)
            view_sql = "ALTER VIEW [%s].[%s] (%s) AS %s" % (user.schema,
                                                            name,
                                                            columns,
                                                            sql)
        else:
            view_sql = "ALTER VIEW [%s].[%s] AS %s" % (user.schema, name, sql)
        try:
            self._create_placeholder_view(name, user)
            self.run_query(view_sql, user, return_cursor=True).close()
        except pyodbc.ProgrammingError as ex:
            # If this is due to not having column names, let's go ahead and
            # make some column names
            problem = str(ex)
            if not re.match('.*no column name was specified', problem):
                raise

            if not column_names:
                cursor = self.run_query(sql, user, return_cursor=True)
                row1 = cursor.fetchone()
                cursor.close()

                columns = map(lambda x: "COLUMN%s" % (x+1), range(len(row1)))
                return self.create_view(name, sql, user, columns)

        count_sql = "SELECT COUNT(*) FROM [%s].[%s]" % (user.schema, name)
        result = self.run_query(count_sql, user)
        return result[0][0]

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

        conn = pyodbc.connect(string, autocommit=True)
        return conn

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
            if "text" == col_type["type"] and col_type["max"] > 0:
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

    def _load_table_sql(self, table_name, row, user, row_count):
        placeholders = map(lambda x: "?", row)
        ph_str = ", ".join(placeholders)

        all_rows = map(lambda x: "(%s)" % ph_str, range(row_count))

        return "INSERT INTO [%s].[%s] VALUES %s" % (user.schema, table_name,
                                                    ", ".join(all_rows))

    def _load_table(self, table_name, data_handle, upload, user):
        data_len = 0
        current_data = []
        sql_max = ""
        max_rows = None
        total_rows_loaded = 0
        current_row = 0

        errors = ""

        def _handle_error_table_set(current_data):
            sql = self._load_table_sql(table_name, current_data[0]["data"], user, 1)
            errors = ""

            for row in current_data:
                try:
                    self.run_query(sql, user, row["data"], return_cursor=True).close()
                except Exception as ex:
                    row_num = row["row"]
                    errors += "Error on row %s: %s\n" % (row_num, str(ex))

            return errors

        for row in data_handle:
            current_row += 1
            if type(row) == str:
                # This is an error in the iteration - eg:
                #  invalid literal for int() with base 10: '72.7273'
                errors += "Error on row %s: %s\n" % (current_row, row)
                continue

            data_len += 1
            current_data.append({"row": current_row, "data": row})

            if not max_rows:
                cols = len(row)
                max_rows = int(MSSQLBackend.MAX_PARAMETERS / cols)

            if data_len == max_rows:
                if not sql_max:
                    sql_max = self._load_table_sql(table_name,
                                                   row, user, max_rows)

                insert_data = []
                for row in current_data:
                    insert_data.extend(row["data"])
                try:
                    self.run_query(sql_max,
                                   user,
                                   insert_data,
                                   return_cursor=True).close()

                except Exception as ex:
                    errors += _handle_error_table_set(current_data)
                current_data = []
                data_len = 0
                total_rows_loaded += max_rows
                upload.rows_loaded = total_rows_loaded
                upload.save()

        if data_len:
            sql = self._load_table_sql(table_name, row, user, data_len)

            insert_data = []
            for row in current_data:
                insert_data.extend(row["data"])
            try:
                self.run_query(sql, user, insert_data, return_cursor=True).close()
            except Exception:
                errors += _handle_error_table_set(current_data)

            total_rows_loaded += data_len
            upload.rows_loaded = total_rows_loaded

            upload.save()

        if errors:
            upload.error = errors
            upload.save()

    def _get_view_sql_for_dataset(self, table_name, user):
        return "SELECT * FROM [%s].[%s]" % (user.schema, table_name)

    def get_query_sample_sql(self, query_id):
        QUERY_SCHEMA = self.get_query_cache_schema_name()
        return "SELECT TOP 100 * FROM [%s].[query_%s]" % (QUERY_SCHEMA,
                                                          query_id)

    def _get_column_definitions_for_cursor(self, cursor):
        import pyodbc
        index = 0
        column_defs = []

        bigint_type = long
        int_type = type(1)
        float_type = type(0.0)
        decimal_type = type(Decimal(0.0))
        boolean_type = type(bool())
        datetime_type = datetime.datetime
        str_type = type("")
        binary_type = buffer

        def make_unique_name(name, existing):
            if name not in existing:
                return name

            return make_unique_name("%s_1" % name, existing)

        existing_column_names = {}
        for col in cursor.description:
            column_name = col[0]
            index = index + 1
            col_type = col[1]
            col_len = col[3]
            null_ok = col[6]

            if column_name == "":
                column_name = "COLUMN%s" % index

            column_name = make_unique_name(column_name, existing_column_names)
            existing_column_names[column_name] = True

            column_name = "[%s]" % (column_name)

            if (col_type == float_type) or (col_type == decimal_type):
                if null_ok:
                    column_defs.append("%s FLOAT" % column_name)
                else:
                    column_defs.append("%s FLOAT NOT NULL" % column_name)

            elif col_type == bigint_type:
                if null_ok:
                    column_defs.append("%s BIGINT " % column_name)
                else:
                    column_defs.append("%s BIGINT NOT NULL" % column_name)

            elif col_type == int_type:
                if null_ok:
                    column_defs.append("%s INT" % column_name)
                else:
                    column_defs.append("%s INT NOT NULL" % column_name)

            elif col_type == datetime_type:
                if null_ok:
                    column_defs.append("%s DATETIME" % column_name)
                else:
                    column_defs.append("%s DATETIME NOT NULL" % column_name)

            elif col_type == boolean_type:
                if null_ok:
                    column_defs.append("%s BIT " % column_name)
                else:
                    column_defs.append("%s BIT NOT NULL" % column_name)

            elif col_type == binary_type:
                if col_len == MSSQLBackend.COLUMN_MAX_LENGTH:
                    type_str = "BINARY(1000)"
                else:
                    type_str = "VARBINARY(%s)" % (col_len)

                if null_ok:
                    column_defs.append("%s %s" % (column_name,
                                                  type_str))
                else:
                    base_str = "%s %s NOT NULL"
                    column_defs.append(base_str % (column_name, type_str))

            elif col_type == str_type and col_len:
                if col_len == MSSQLBackend.COLUMN_MAX_LENGTH:
                    type_str = "TEXT"
                else:
                    type_str = "VARCHAR(%s)" % (col_len)

                if null_ok:
                    column_defs.append("%s %s" % (column_name,
                                                  type_str))
                else:
                    base_str = "%s %s NOT NULL"
                    column_defs.append(base_str % (column_name, type_str))
            else:
                column_defs.append("%s TEXT" % column_name)

        return ", ".join(column_defs)

    def remove_table_for_query_by_name(self, name):
        try:
            QUERY_SCHEMA = self.get_query_cache_schema_name()
            cursor = connection.cursor()
            full_name = "[%s].[%s]" % (QUERY_SCHEMA, name)
            drop_table = "DROP TABLE %s" % (full_name)
            cursor.execute(drop_table)
        except:
            pass

    def create_table_from_query_result(self, name, source_cursor):
        # Make sure the db exists to stash query results into
        QUERY_SCHEMA = self.get_query_cache_schema_name()
        cursor = connection.cursor()

        sql = "SELECT name FROM sys.schemas WHERE name = ?"
        cursor.execute(sql, (QUERY_SCHEMA, ))

        if not cursor.rowcount:
            cursor.execute("CREATE SCHEMA %s" % (QUERY_SCHEMA))

        column_def = self._get_column_definitions_for_cursor(source_cursor)

        full_name = "[%s].[%s]" % (QUERY_SCHEMA, name)
        create_table = "CREATE TABLE %s (%s)" % (full_name, column_def)

        cursor.execute(create_table)

        row = source_cursor.fetchone()

        if row is None:
            return 0

        placeholders = ", ".join(list(map(lambda x: "?", row)))
        insert = "INSERT INTO %s VALUES (%s)" % (full_name, placeholders)
        row_count = 0
        while row:
            cursor.execute(insert, row)
            row = source_cursor.fetchone()
            row_count += 1
        return row_count

    def get_query_cache_schema_name(self):
        return getattr(settings, "SQLSHARE_QUERY_CACHE_SCHEMA", "QUERY_SCHEMA")

    def get_qualified_name(self, dataset):
        return "[%s].[%s]" % (dataset.owner.schema, dataset.name)

    def get_download_sql_for_dataset(self, dataset):
        return "SELECT * FROM %s" % self.get_qualified_name(dataset)

    def get_preview_sql_for_dataset(self, dataset_name, user):
        return "SELECT TOP 100 * FROM [%s].[%s]" % (user.schema, dataset_name)

    def get_preview_sql_for_query(self, sql):
        return "SELECT TOP 100 * FROM (%s) as x" % sql

    def delete_table(self, table, owner):
        sql = "DROP TABLE [%s].[%s]" % (owner.schema, table)
        self.run_query(sql, owner, return_cursor=True).close()

    def delete_dataset(self, dataset_name, owner):
        sql = "DROP VIEW [%s].[%s]" % (owner.schema, dataset_name)
        self.run_query(sql, owner, return_cursor=True).close()

    def _add_read_access_sql(self, dataset, owner, reader):
        return "GRANT SELECT ON [%s].[%s] TO %s" % (owner.schema,
                                                    dataset,
                                                    reader.db_username)

    def _remove_read_access_sql(self, dataset, owner, reader):
        return "REVOKE ALL ON [%s].[%s] FROM %s" % (owner.schema,
                                                    dataset,
                                                    reader.db_username)

    def _read_access_to_query_sql(self, query_id, user):
        db = self.get_query_cache_schema_name()
        return "GRANT SELECT ON [%s].[query_%s] TO [%s]" % (db,
                                                            query_id,
                                                            user.db_username)

    def _owner_read_access_to_query_sql(self, query_id, user):
        db = self.get_query_cache_schema_name()
        return ("GRANT SELECT ON [%s].[query_%s] TO "
                "[%s] WITH GRANT OPTION") % (db,
                                             query_id,
                                             user.db_username)

    def add_owner_read_access_to_query(self, query_id, user):
        sql = self._owner_read_access_to_query_sql(query_id, user)
        cursor = connection.cursor()
        cursor.execute(sql)

    def add_read_access_to_query(self, query_id, user):
        sql = self._read_access_to_query_sql(query_id, user)
        cursor = connection.cursor()
        cursor.execute(sql)

    def add_read_access_to_dataset(self, dataset, owner, reader):
        # test round one:
        sql = self._add_read_access_sql(dataset, owner, reader)
        self.run_query(sql, owner, return_cursor=True).close()

    def remove_access_to_dataset(self, dataset, owner, reader):
        sql = self._remove_read_access_sql(dataset, owner, reader)
        self.run_query(sql, owner, return_cursor=True).close()

    def _add_public_access_sql(self, dataset, owner):
        return "GRANT SELECT ON [%s].[%s] TO PUBLIC" % (owner.schema,
                                                        dataset.name)

    def _add_public_access_to_sample(self, dataset):
        schema = self.get_query_cache_schema_name()
        try:
            query = Query.objects.get(is_preview_for=dataset.pk)
        except Query.DoesNotExist:
            return
        sample_id = query.pk

        if query.is_finished:
            return "GRANT SELECT ON [%s].[query_%s] TO PUBLIC" % (schema,
                                                                  sample_id)

    def add_public_access(self, dataset, owner):
        sql = self._add_public_access_sql(dataset, owner)
        self.run_query(sql, owner, return_cursor=True).close()
        # Add public access to the data sample
        sql = self._add_public_access_to_sample(dataset)
        if sql:
            self.run_query(sql, owner, return_cursor=True).close()

    def _remove_public_access_sql(self, dataset, owner):
        return "REVOKE ALL ON [%s].[%s] FROM PUBLIC" % (owner.schema,
                                                        dataset.name)

    def _remove_public_access_from_sample(self, dataset):
        schema = self.get_query_cache_schema_name()
        if dataset.preview_is_finished:
            query = Query.objects.get(is_preview_for=dataset)
            sample_id = query.pk

            return "REVOKE SELECT ON [%s].[query_%s] FROM PUBLIC" % (schema,
                                                                     sample_id)

    def remove_public_access(self, dataset, owner):
        sql = self._remove_public_access_sql(dataset, owner)
        self.run_query(sql, owner, return_cursor=True).close()

        sql = self._remove_public_access_from_sample(dataset)
        if sql:
            self.run_query(sql, owner, return_cursor=True).close()

    def get_running_queries(self):
        query = """SELECT sqltext.TEXT as sql,
        req.session_id,
        req.status,
        req.command,
        req.cpu_time,
        req.total_elapsed_time
        FROM sys.dm_exec_requests req
        CROSS APPLY sys.dm_exec_sql_text(sql_handle) AS sqltext"""

        cursor = connection.cursor()
        cursor.execute(query)

        queries = []
        row = cursor.fetchone()
        while row:
            queries.append({"sql": row[0]})
            row = cursor.fetchone()

        return queries


class SQLAzureBackend(MSSQLBackend):
    """
    Essentially just the MS SQL Backend.  Some changes are needed for user
    management though.
    """
    def create_db_user(self, username, password):
        import pyodbc
        pyodbc.pooling = False

        string = "DSN=%s;UID=%s;PWD=%s;DATABASE=%s;%s" % (
            settings.DATABASES['default']['OPTIONS']['dsn'],
            settings.DATABASES['default']['USER'],
            settings.DATABASES['default']['PASSWORD'],
            'master',
            settings.DATABASES['default']['OPTIONS']['extra_params'],
            )

        master_conn = pyodbc.connect(string, autocommit=True)

        ret = self._run_create_db_user(master_conn, username, password)
        master_conn.close()
        return ret

    def remove_db_user(self, user):
        """
        The rules for being able to remove a user seem to be different enough
        in azure that i'm just passing on it.
        """
        pass

    def get_testing_time_delta_limit(self):
        """
        For some reason, at least from my home connection, these tests can
        take a long time.  This is a larger value than needed, but i didn't
        want to fiddle with it.
        """
        return 120
