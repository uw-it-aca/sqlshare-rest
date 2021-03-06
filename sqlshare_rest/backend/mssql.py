from sqlshare_rest.backend.base import DBInterface
from sqlshare_rest.models import User, Query
from logging import getLogger
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
logger = getLogger(__name__)


class MSSQLBackend(DBInterface):
    COLUMN_MAX_LENGTH = 2147483647
    MAX_PARAMETERS = 2099
    # This gave a good balance between speed, and not needing to do too many
    # slow inserts on dirty data.
    MAX_EXECUTE_MANY = 100

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
        cursor = connection.cursor()
        sql = "GRANT SHOWPLAN TO [%s]" % (username)
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

    def get_query_plan(self, sql, user):
        try:
            connection = self.get_connection_for_user(user)
            cursor = connection.cursor()
            cursor.execute("SET SHOWPLAN_XML ON")
            try:
                cursor.execute(sql)
                data = cursor.fetchall()[0][0]
            except Exception:
                raise
            finally:
                cursor.execute("SET SHOWPLAN_XML OFF")

            cursor.close()
            return data
        except Exception as ex:
            # Need to sort out a permissions issue
            return ""

    def run_query_many(self, sql, user, params=None, return_cursor=False):
        connection = self.get_connection_for_user(user)
        original_autocommit = connection.autocommit
        try:
            connection.autocommit = False
            cursor = connection.cursor()
            if params:
                # Because, seriously:
                # 'The SQL contains 0 parameter markers,
                # but 1 parameters were supplied'
                cursor.executemany(sql, params)
            else:
                cursor.executemany(sql)

            connection.commit()
            if return_cursor:
                return cursor
            data = cursor.fetchall()
            cursor.close()
            return data
        except Exception as ex:
            connection.rollback()
            raise
        finally:
            connection.autocommit = original_autocommit

    def run_query(self, sql, user, params=None, return_cursor=False,
                  query=None):
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

        conn = pyodbc.connect(string, autocommit=True, unicode_results=True,
                              ansi=False)
        return conn

    def _make_safe_column_name_list(self, names):
        output_names = []
        seen_names = {}
        for name in names:
            name = name.replace("[", "(")
            name = name.replace("]", ")")

            if name == "":
                name = "COLUMN"

            unique_name = self.make_unique_name(name, seen_names)
            seen_names[unique_name] = True
            output_names.append(unique_name)

        return output_names

    def _create_table(self, table_name, column_names, column_types, user):
        column_names = self._make_safe_column_name_list(column_names)
        # Create a table that has the correctly typed data
        try:
            sql = self._create_table_sql(user,
                                         table_name,
                                         column_names,
                                         column_types)

            self.run_query(sql, user, return_cursor=True).close()
        except Exception as ex:
            # We expect tables to already exist - uploading a replacement
            # file gives that exception.  Anything else though is probably
            # a bug worth looking into.
            ex_str = str(ex)
            if not ex_str.find("There is already an object named"):
                logger.error("Error creating table: %s" % str(ex))
            drop_sql = self._drop_exisisting_table_sql(user, table_name)
            self.run_query(drop_sql, user, return_cursor=True).close()
            self.run_query(sql, user, return_cursor=True).close()

        # Create a second table that has ... everything we were wrong about
        try:
            sql = self._create_untyped_table_sql(user,
                                                 table_name,
                                                 column_names,
                                                 column_types)

            self.run_query(sql, user, return_cursor=True).close()
        except Exception as ex:
            # We expect tables to already exist - uploading a replacement
            # file gives that exception.  Anything else though is probably
            # a bug worth looking into.
            ex_str = str(ex)
            if not ex_str.find("There is already an object named"):
                logger.error("Error creating table: %s" % str(ex))
            drop_sql = self._drop_exisisting_untyped_table_sql(user,
                                                               table_name)
            self.run_query(drop_sql, user, return_cursor=True).close()
            self.run_query(sql, user, return_cursor=True).close()

    def _drop_exisisting_table_sql(self, user, table_name):
        return "DROP TABLE [%s].[%s]" % (user.schema, table_name)

    def _drop_exisisting_untyped_table_sql(self, user, table_name):
        return "DROP TABLE [%s].[untyped_%s]" % (user.schema, table_name)

    def _create_table_sql(self, user, table_name, column_names, column_types):
        def _column_sql(name, col_type):
            if "int" == col_type["type"]:
                return "[%s] int" % name
            if "float" == col_type["type"]:
                return "[%s] float" % name
            if "text" == col_type["type"] and col_type["max"] > 0:
                return "[%s] nvarchar(MAX)" % (name)
            # Fallback to text is hopefully good?
            return "[%s] nvarchar(MAX)" % name

        columns = []
        for i in range(0, len(column_names)):
            columns.append(_column_sql(column_names[i], column_types[i]))

        return "CREATE TABLE [%s].[%s] (%s)" % (
                    user.schema,
                    table_name,
                    ", ".join(columns)
               )

    def _create_untyped_table_sql(self, user, table_name, names, types):
        columns = []
        for i in range(0, len(names)):
            name = names[i]
            columns.append("[%s] nvarchar(MAX)" % name)

        return "CREATE TABLE [%s].[untyped_%s] (%s)" % (
                    user.schema,
                    table_name,
                    ", ".join(columns)
               )

    def _load_table_untyped_sql(self, table_name, row, user, row_count):
        placeholders = map(lambda x: "?", row)
        ph_str = ", ".join(placeholders)

        all_rows = map(lambda x: "(%s)" % ph_str, range(row_count))
        placeholders = ", ".join(all_rows)

        return "INSERT INTO [%s].[untyped_%s] VALUES %s" % (user.schema,
                                                            table_name,
                                                            placeholders)

    def _load_table_sql(self, table_name, row, user, row_count):
        placeholders = map(lambda x: "?", row)
        ph_str = ", ".join(placeholders)

        all_rows = map(lambda x: "(%s)" % ph_str, range(row_count))

        return "INSERT INTO [%s].[%s] VALUES %s" % (user.schema, table_name,
                                                    ", ".join(all_rows))

    def _load_table(self, table_name, data_handle, upload, user):
        data_len = 0
        current_data = []
        insert_multi_sql = ""
        max_rows = None
        total_rows_loaded = 0
        current_row = 0

        errors = ""

        def _handle_error_table_set(current_data):
            sql = self._load_table_sql(table_name,
                                       current_data[0]["data"],
                                       user,
                                       1)

            untyped_sql = self._load_table_untyped_sql(table_name,
                                                       current_data[0]["data"],
                                                       user,
                                                       1)
            errors = ""

            for row in current_data:
                try:
                    self.run_query(sql,
                                   user,
                                   row["data"],
                                   return_cursor=True).close()
                except Exception as ex:
                    row_num = row["row"]
                    self.run_query(untyped_sql,
                                   user,
                                   row["data"],
                                   return_cursor=True).close()

            return errors

        max_rows = MSSQLBackend.MAX_EXECUTE_MANY

        for row in data_handle:
            if not insert_multi_sql:
                insert_multi_sql = self._load_table_sql(table_name,
                                                        row,
                                                        user, 1)
            current_row += 1
            if type(row) == str:
                # This is an error in the iteration - eg:
                #  invalid literal for int() with base 10: '72.7273'
                errors += "Error on row %s: %s\n" % (current_row, row)
                continue

            data_len += 1
            current_data.append({"row": current_row, "data": row})

            if data_len == max_rows:
                insert_data = []
                for row in current_data:
                    insert_data.append(row["data"])
                try:
                    self.run_query_many(insert_multi_sql,
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
            insert_data = []
            for row in current_data:
                insert_data.append(row["data"])
            try:
                self.run_query_many(insert_multi_sql,
                                    user,
                                    insert_data,
                                    return_cursor=True).close()
            except Exception as ex:
                errors += _handle_error_table_set(current_data)

            total_rows_loaded += data_len
            upload.rows_loaded = total_rows_loaded

            upload.save()

        if errors:
            upload.error = errors
            upload.save()

    def create_dataset_from_parser(self, dataset_name, parser, upload, user):
        """
        OVERRIDING THE BASE METHOD!

        Our datset sql needs to be column aware, because we're storing data
        in 2 tables, one that's clean, one that isn't.  The union needs to do
        per-column casts of the clean data.

        Turns a parser object into a dataset.  This process should update
        the rows_loaded attribute of the upload object as it is processed.
        rows_loaded will be set to rows_total at the end of
        create_table_from_parser().

        # Creates a table based on the parser columns
        # Loads the data that's in the handle for the parser
        # Creates the view for the dataset
        """
        table_name = self.create_table_from_parser(dataset_name,
                                                   parser,
                                                   upload,
                                                   user)
        upload.save()
        self.create_view(dataset_name,
                         self._get_view_sql_for_dataset_by_parser(table_name,
                                                                  parser,
                                                                  user),
                         user)

    def get_view_sql_for_dataset_by_parser(self, table_name, parser, user):
        return self._get_view_sql_for_dataset_by_parser(table_name,
                                                        parser,
                                                        user)

    def _get_view_sql_for_dataset_by_parser(self, table_name, parser, user):
        cast = []
        plain = []
        base = []

        base = parser.column_names()
        base.append('clean')
        all_unique = parser.make_unique_columns(base)
        all_unique = self._make_safe_column_name_list(all_unique)

        for c in all_unique[0:-1]:
            cast.append("CAST([%s] AS NVARCHAR(MAX)) AS [%s]" % (c, c))
            plain.append("[%s]" % c)
            base.append(c)

        clean_col = all_unique[-1]

        cast.append("1 as %s" % (clean_col))
        plain.append("0 as %s" % (clean_col))

        cast_columns = "\n     , ".join(cast)
        plain_columns = "\n     , ".join(plain)

        args = (cast_columns, user.schema, table_name,
                plain_columns, user.schema, table_name)

        return ("SELECT %s\n  FROM [%s].[%s]\nUNION ALL\n"
                "SELECT %s\n  FROM [%s].[untyped_%s]") % args

    def _get_view_sql_for_dataset(self, table_name, user):
        return "SELECT * FROM [%s].[%s]" % (user.schema, table_name)

    def make_unique_name(self, name, existing):
        """
        Given a name and a dictionary of existing names, returns a name
        that will be unique when added to the dictionary.
        """
        if name not in existing:
            return name

        return self.make_unique_name("%s_1" % name, existing)

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

        existing_column_names = {}
        for col in cursor.description:
            column_name = col[0]
            index = index + 1
            col_type = col[1]
            col_len = col[3]
            null_ok = col[6]

            if column_name == "":
                column_name = "COLUMN%s" % index

            column_name = self.make_unique_name(column_name,
                                                existing_column_names)
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
                    type_str = "NVARCHAR(%s)" % (col_len)

                if null_ok:
                    column_defs.append("%s %s" % (column_name,
                                                  type_str))
                else:
                    base_str = "%s %s NOT NULL"
                    column_defs.append(base_str % (column_name, type_str))
            else:
                column_defs.append("%s TEXT" % column_name)

        return ", ".join(column_defs)

    def create_table_from_query_result(self, name, source_cursor):
        # Make sure the db exists to stash query results into
        QUERY_SCHEMA = self.get_query_cache_schema_name()
        cursor = connection.cursor()
        import time

        t1 = time.time()
        sql = "SELECT name FROM sys.schemas WHERE name = ?"
        cursor.execute(sql, (QUERY_SCHEMA, ))

        if not cursor.rowcount:
            cursor.execute("CREATE SCHEMA %s" % (QUERY_SCHEMA))

        t2 = time.time()
        column_def = self._get_column_definitions_for_cursor(source_cursor)
        t3 = time.time()

        full_name = "[%s].[%s]" % (QUERY_SCHEMA, name)
        create_table = "CREATE TABLE %s (%s)" % (full_name, column_def)

        cursor.execute(create_table)
        t4 = time.time()

        def _insert_sql(table_name, row, row_count):
            placeholders = map(lambda x: "?", row)
            ph_str = ", ".join(placeholders)

            all_rows = map(lambda x: "(%s)" % ph_str, range(row_count))

            return "INSERT INTO %s VALUES %s" % (table_name,
                                                 ", ".join(all_rows))

        try:
            # XXX - refactor with _load_table
            data_len = 0
            current_data = []
            sql_max = ""
            max_rows = None
            total_rows_loaded = 0
            current_row = 0

            for row in source_cursor:
                current_row += 1

                data_len += 1
                current_data.append({"row": current_row, "data": row})

                if not max_rows:
                    cols = len(row)
                    max_rows = int(MSSQLBackend.MAX_PARAMETERS / cols)

                if data_len == max_rows:
                    if not sql_max:
                        sql_max = _insert_sql(full_name, row, max_rows)

                    insert_data = []
                    for row in current_data:
                        insert_data.extend(row["data"])

                    cursor.execute(sql_max, insert_data)

                    current_data = []
                    data_len = 0
                    total_rows_loaded += max_rows

            if data_len:
                sql = _insert_sql(full_name, row, data_len)

                insert_data = []
                for row in current_data:
                    insert_data.extend(row["data"])
                cursor.execute(sql, insert_data)

                total_rows_loaded += data_len
        except Exception as ex:
            print("Ex: ", (str(ex)))

        t5 = time.time()

        return total_rows_loaded

    def get_query_cache_schema_name(self):
        return getattr(settings, "SQLSHARE_QUERY_CACHE_SCHEMA", "QUERY_SCHEMA")

    def get_qualified_name(self, dataset):
        return "[%s].[%s]" % (dataset.owner.schema, dataset.name)

    def get_download_sql_for_dataset(self, dataset):
        return "SELECT * FROM %s" % self.get_qualified_name(dataset)

    def get_preview_sql_for_dataset(self, dataset_name, user):
        return "SELECT TOP 100 * FROM [%s].[%s]" % (user.schema, dataset_name)

    def get_preview_sql_for_query(self, sql):
        try:
            # We only want to modify select statements, though that is the
            # 'expected' type of query here...
            if re.match('\s*select\s', sql, re.IGNORECASE):
                # SQLSHR-222 - TOP and DISTINCT don't go together
                # this is probably overly broad, but it seems better to
                # select too much than to reject a query
                if re.match('.*distinct', sql, re.IGNORECASE):
                    return sql
                # If they already have a TOP value in their query we need to
                # modify it
                if re.match('\s*select\s+top', sql, re.IGNORECASE):
                    # Don't allow the top percentage syntax
                    percent_string = '\s*select\s+top\s*\(?[\d]+\)?\s+percent'
                    if re.match(percent_string, sql, re.IGNORECASE):
                        sub_str = '(?i)%s\s*' % percent_string
                        return re.sub(sub_str, 'SELECT TOP 100 ', sql, count=1)
                    else:
                        test = '\s*select\s+top\s*\(?([\d]+)\)?\s+'
                        value = re.match(test, sql, re.IGNORECASE).group(1)

                        # They're limiting more than we would, no problem
                        if int(value) <= 100:
                            return sql

                        # String out their limit, add ours
                        replace = '(?i)%s' % test
                        return re.sub(replace, 'SELECT TOP 100 ', sql, count=1)

                else:
                    # Otherwise, just add the top 100.
                    return re.sub('(?i)\s*select\s+', 'SELECT TOP 100 ',
                                  sql, count=1)
        except Exception as ex:
            print("Error on sql statement %s: %s", sql, str(ex))
            return sql

        return sql

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

    def _add_read_access_sql_to_table(self, dataset, owner, reader):
        return "GRANT SELECT ON [%s].[table_%s] TO %s" % (owner.schema,
                                                          dataset,
                                                          reader.db_username)

    def _add_read_access_sql_to_untyped(self, dataset, owner, reader):
        username = reader.db_username
        return "GRANT SELECT ON [%s].[untyped_table_%s] TO %s" % (owner.schema,
                                                                  dataset,
                                                                  username)

    def _remove_read_access_sql(self, dataset, owner, reader):
        return "REVOKE ALL ON [%s].[%s] FROM %s" % (owner.schema,
                                                    dataset,
                                                    reader.db_username)

    def _remove_read_access_sql_from_table(self, dataset, owner, reader):
        return "REVOKE ALL ON [%s].[table_%s] FROM %s" % (owner.schema,
                                                          dataset,
                                                          reader.db_username)

    def _remove_read_access_sql_from_untyped(self, dataset, owner, reader):
        username = reader.db_username
        return "REVOKE ALL ON [%s].[untyped_table_%s] FROM %s" % (owner.schema,
                                                                  dataset,
                                                                  username)

    def add_read_access_to_dataset(self, dataset, owner, reader):
        # test round one:
        sql = self._add_read_access_sql(dataset, owner, reader)
        self.run_query(sql, owner, return_cursor=True).close()

        # If there's a backing table, grant select to that as well...
        sql = self._add_read_access_sql_to_table(dataset, owner, reader)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception:
            pass
        # If there's a backing table, grant select to that as well...
        sql = self._add_read_access_sql_to_untyped(dataset,
                                                   owner,
                                                   reader)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception:
            pass

    def remove_access_to_dataset(self, dataset, owner, reader):
        sql = self._remove_read_access_sql(dataset, owner, reader)
        self.run_query(sql, owner, return_cursor=True).close()
        # If there's a backing table, drom select from that as well...
        sql = self._remove_read_access_sql_from_table(dataset, owner, reader)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception:
            pass

        sql = self._remove_read_access_sql_from_untyped(dataset,
                                                        owner,
                                                        reader)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception:
            pass

    def _add_public_access_sql(self, dataset, owner):
        return "GRANT SELECT ON [%s].[%s] TO PUBLIC" % (owner.schema,
                                                        dataset.name)

    def _add_public_access_sql_to_table(self, dataset, owner):
        return "GRANT SELECT ON [%s].[table_%s] TO PUBLIC" % (owner.schema,
                                                              dataset.name)

    def _add_public_access_sql_to_untyped(self, dataset, owner):
        user = owner.schema
        name = dataset.name
        return "GRANT SELECT ON [%s].[untyped_table_%s] TO PUBLIC" % (user,
                                                                      name)

    def add_public_access(self, dataset, owner):
        sql = self._add_public_access_sql(dataset, owner)
        self.run_query(sql, owner, return_cursor=True).close()

        # If there's a backing table, grant select to public as well...
        sql = self._add_public_access_sql_to_table(dataset, owner)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception:
            pass
        sql = self._add_public_access_sql_to_untyped(dataset, owner)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception:
            pass

    def _remove_public_access_sql(self, dataset, owner):
        return "REVOKE ALL ON [%s].[%s] FROM PUBLIC" % (owner.schema,
                                                        dataset.name)

    def _remove_public_access_sql_from_table(self, dataset, owner):
        return "REVOKE ALL ON [%s].[table_%s] FROM PUBLIC" % (owner.schema,
                                                              dataset.name)

    def _remove_public_access_sql_from_untyped(self, dataset, owner):
        user = owner.schema
        name = dataset.name
        return "REVOKE ALL ON [%s].[untyped_table_%s] FROM PUBLIC" % (user,
                                                                      name)

    def remove_public_access(self, dataset, owner):
        sql = self._remove_public_access_sql(dataset, owner)
        self.run_query(sql, owner, return_cursor=True).close()

        # If there's a backing table, drop select from public as well...
        sql = self._remove_public_access_sql_from_table(dataset, owner)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception:
            pass

        sql = self._remove_public_access_sql_from_untyped(dataset, owner)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception:
            pass

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
