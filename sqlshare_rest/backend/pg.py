from sqlshare_rest.backend.base import DBInterface
from sqlshare_rest.models import User
from django.db import connection
from django.conf import settings
from logging import getLogger
import re
import tempfile

logger = getLogger(__name__)


class PGBackend(DBInterface):
    def create_db_user(self, username, password):
        cursor = connection.cursor()
        sql = "CREATE USER %s WITH PASSWORD '%s'"
        cursor.execute(sql % (username, password))
        cursor.close()

    def create_db_schema(self, username, schema):
        cursor = connection.cursor()
        sql = "CREATE SCHEMA %s AUTHORIZATION %s"
        cursor.execute(sql % (schema, username))
        cursor.close()

        # Make sure people can see items in the schema
        sql = 'GRANT USAGE ON SCHEMA %s to PUBLIC' % (schema)
        self.run_query(sql, owner, return_cursor=True).close()

    def remove_user(self, username):
        model = User.objects.get(username=username)
        self.close_user_connection(model)
        try:
            self.remove_schema(model.schema)
        except Exception as ex:
            print "Error removing schema for username %s: " % username, ex
        self.remove_db_user(model.db_username)

    def remove_db_user(self, user):
        sql = "DROP OWNED BY %s CASCADE" % user
        cursor = connection.cursor()
        cursor.execute(sql)
        cursor.close()

        sql = "DROP ROLE %s" % user
        cursor = connection.cursor()
        cursor.execute(sql)
        cursor.close()

    def remove_schema(self, schema):
        sql = "DROP SCHEMA %s CASCADE" % schema
        cursor = connection.cursor()
        cursor.execute(sql)
        cursor.close()

    def run_query(self, sql, user, params=None, return_cursor=False,
                  query=None):
        connection = self.get_connection_for_user(user)

        if query:
            query.backend_terminate_data = "%s" % connection.get_backend_pid()
            query.save()

        cursor = connection.cursor()
        cursor.execute(sql, params)

        if return_cursor:
            return cursor
        return cursor.fetchall()

    def _create_view_sql(self, schema, name, sql):
        return 'CREATE VIEW %s."%s" AS %s' % (schema, name, sql)

    def _drop_view_sql(self, schema, name):
        return 'DROP VIEW %s."%s" CASCADE' % (schema, name)

    def _drop_table_sql(self, schema, name):
        return 'DROP TABLE %s."%s" CASCADE' % (schema, name)

    def delete_table(self, dataset_name, owner):
        sql = self._drop_table_sql(owner.schema, dataset_name)
        self.run_query(sql, owner, return_cursor=True).close()

    def delete_dataset(self, dataset_name, owner):
        sql = self._drop_view_sql(owner.schema, dataset_name)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception as ex:
            print "Error deleting: ", ex

    def create_view(self, name, sql, user):
        schema = user.schema
        view_sql = self._create_view_sql(schema, name, sql)
        try:
            self.run_query(view_sql, user, return_cursor=True).close()
        except Exception as ex:
            try:
                drop_sql = self._drop_view_sql(schema, name)
                try:
                    self.run_query(drop_sql, user, return_cursor=True).close()
                except:
                    # We don't care if there's an error trying to drop the
                    # view.  It might Not exist.  We want the exception that
                    # goes back to be the creation exception.
                    pass
                self.run_query(view_sql, user, return_cursor=True).close()
            except:
                raise

        count_sql = 'SELECT COUNT(*) FROM %s."%s"' % (schema, name)

        result = self.run_query(count_sql, user)
        return result[0][0]

    def _get_snapshot_view_sql(self, dataset):
        table_name = self._get_table_name_for_dataset(dataset.name)
        return ('CREATE VIEW %s."%s" AS '
                'SELECT * FROM %s."%s"' % (dataset.owner.schema,
                                           dataset.name,
                                           dataset.owner.schema,
                                           table_name))

    def _create_view_of_snapshot(self, dataset, user):
        sql = self._get_snapshot_view_sql(dataset)
        self.run_query(sql, user, return_cursor=True).close()

    def _create_snapshot_table(self, source_dataset, table_name, user):
        source_schema = source_dataset.owner.schema
        base = 'CREATE TABLE %s."%s" AS SELECT * FROM %s."%s"'
        sql = base % (user.schema,
                      table_name,
                      source_schema,
                      source_dataset.name)

        self.run_query(sql, user, return_cursor=True).close()

    def add_public_access(self, dataset, owner):
        # Granting to the view
        sql = 'GRANT SELECT ON %s."%s" to PUBLIC' % (owner.schema,
                                                     dataset.name)

        self.run_query(sql, owner, return_cursor=True).close()

        # Granting to the underlying table, if one exists
        table_name = self._get_table_name_for_dataset(dataset.name)
        sql = 'GRANT SELECT ON %s."%s" to PUBLIC' % (owner.schema,
                                                     table_name)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception as ex:
            pass

        # Granting to the underlying untyped table, if one exists
        table_name = self._get_table_name_for_dataset(dataset.name)
        sql = 'GRANT SELECT ON %s."untyped_%s" to PUBLIC' % (owner.schema,
                                                             table_name)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception as ex:
            pass

    def remove_public_access(self, dataset, owner):
        sql = 'REVOKE SELECT ON %s."%s" FROM PUBLIC' % (owner.schema,
                                                        dataset.name)
        self.run_query(sql, owner, return_cursor=True).close()

        # dropping from the underlying table, if one exists
        table_name = self._get_table_name_for_dataset(dataset.name)
        sql = 'REVOKE SELECT ON %s."%s" FROM PUBLIC' % (owner.schema,
                                                        table_name)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception as ex:
            pass

        # dropping from the underlying untyped table, if one exists
        table_name = self._get_table_name_for_dataset(dataset.name)
        sql = 'REVOKE SELECT ON %s."untyped_%s" FROM PUBLIC' % (owner.schema,
                                                                table_name)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception as ex:
            pass

    def add_read_access_to_dataset(self, dataset, owner, reader):
        sql = 'GRANT SELECT ON %s."%s" to %s' % (owner.schema, dataset,
                                                 reader.db_username)
        self.run_query(sql, owner, return_cursor=True).close()

        # Granting to the underlying table, if one exists
        table_name = self._get_table_name_for_dataset(dataset)
        sql = 'GRANT SELECT ON %s."%s" to %s' % (owner.schema,
                                                 table_name,
                                                 reader.db_username)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception as ex:
            pass

        # Granting to the underlying untyped table, if one exists
        sql = 'GRANT SELECT ON %s."untyped_%s" to %s' % (owner.schema,
                                                         table_name,
                                                         reader.db_username)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception as ex:
            pass

    def remove_access_to_dataset(self, dataset, owner, reader):
        sql = 'REVOKE ALL ON %s."%s" FROM %s' % (owner.schema, dataset,
                                                 reader.db_username)
        self.run_query(sql, owner, return_cursor=True).close()

        # dropping from the underlying table, if one exists
        table_name = self._get_table_name_for_dataset(dataset)
        sql = 'REVOKE SELECT ON %s."%s" FROM %s' % (owner.schema,
                                                    table_name,
                                                    reader.db_username)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception as ex:
            pass

        # dropping from the underlying untyped table, if one exists
        sql = 'REVOKE SELECT ON %s."untyped_%s" FROM %s' % (owner.schema,
                                                            table_name,
                                                            reader.db_username)
        try:
            self.run_query(sql, owner, return_cursor=True).close()
        except Exception as ex:
            pass

    def get_preview_sql_for_query(self, sql):
        return "SELECT * FROM (%s) AS X LIMIT 100" % sql

    def get_preview_sql_for_dataset(self, dataset_name, user):
        return 'SELECT * FROM %s."%s" LIMIT 100' % (user.schema, dataset_name)

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

    def create_table_from_parser(self, dataset_name, parser, upload, user):
        table_name = self._get_table_name_for_dataset(dataset_name)
        self._create_table(table_name=table_name,
                           column_names=parser.column_names(),
                           column_types=parser.column_types(),
                           user=user)

        # We need the parser to get column types!
        self._load_table(table_name, parser, upload, user)

        upload.rows_loaded = upload.rows_total
        return table_name

    def get_view_sql_for_dataset_by_parser(self, table_name, parser, user):
        return self._get_view_sql_for_dataset_by_parser(table_name,
                                                        parser,
                                                        user)

    def _get_view_sql_for_dataset(self, table_name, user):
        return 'SELECT * FROM %s."%s"' % (user.schema, table_name)

    def get_download_sql_for_dataset(self, dataset):
        return 'SELECT * FROM %s' % self.get_qualified_name(dataset)

    def get_qualified_name(self, dataset):
        return '%s."%s"' % (dataset.owner.schema, dataset.name)

    def _get_insert_statement(self, user, table_name, data):
        row = data[0]
        placeholders = map(lambda x: "%s", row)
        ph_str = ", ".join(placeholders)

        multi_ph = ", ".join(["(%s)" % ph_str] * len(data))

        insert_str = 'INSERT INTO %s."%s" VALUES %s' % (user.schema,
                                                        table_name,
                                                        multi_ph)
        return insert_str

    def _get_fallback_insert(self, user, table_name):
        return 'INSERT INTO %s."%s" VALUES (%s)' % (user.schema,
                                                    table_name,
                                                    '%s')

    def _load_table(self, table_name, parser, upload, user):
        # for _load_table_inserts, if we need it.
        data_handle = parser.get_data_handle()
        return self._load_table_copy(table_name, parser, upload, user)

    def _load_table_copy(self, table_name, parser, upload, user):
        valid_data_temp = tempfile.NamedTemporaryFile()
        bad_data_temp = tempfile.NamedTemporaryFile()

        column_types = parser.column_types()
        data_handle = parser.get_data_handle()

        col_type_len = len(column_types)

        count = 0
        total_lines = upload.rows_total

        for row in data_handle:
            count += 1
            if not count % 1000:
                upload.rows_loaded = count
                upload.save()

            row_len = len(row)
            good_row = True
            for index in range(0, col_type_len):
                ctype = column_types[index]
                col_type = ctype["type"]
                if index >= row_len:
                    row.append("\\N")
                else:
                    value = row[index]
                    if col_type == "int":
                        try:
                            int(value)
                        except Exception:
                            good_row = False
                    elif col_type == "float":
                        try:
                            float(value)
                        except Exception:
                            good_row = False
                    elif col_type == "text":
                        value = value.replace("\t", "\\t")
                        value = value.replace("\n", "\\n")
                        row[index] = value
                    else:
                        raise Exception("Unknown type: %s" % col_type)

            if row_len > col_type_len:
                bad_line = ",".join(row)[:8000]+"..."
                bad_line += "\t\\N" * col_type_len - 1
                bad_data_temp.write(bad_line+"\n")
            elif good_row:
                valid_data_temp.write("\t".join(row) + "\n")
            else:
                bad_data_temp.write("\t".join(row) + "\n")

        bad_data_temp.seek(0)
        valid_data_temp.seek(0)
        connecteon = self.get_connection_for_user(user)
        cursor = connection.cursor()
        cursor.copy_from(valid_data_temp, '%s."%s"' % (user.schema,
                                                       table_name))
        cursor.close()
        cursor = connection.cursor()
        cursor.copy_from(bad_data_temp, '%s."untyped_%s"' % (user.schema,
                                                             table_name))
        cursor.close()

    def make_unique_name(self, name, existing):
        """
        Given a name and a dictionary of existing names, returns a name
        that will be unique when added to the dictionary.
        """
        if name not in existing:
            return name

        return self.make_unique_name("%s_1" % name, existing)

    def _make_safe_column_name_list(self, names):
        output_names = []
        seen_names = {}
        for name in names:
            if name == "":
                name = "COLUMN"

            unique_name = self.make_unique_name(name, seen_names)
            seen_names[unique_name] = True
            output_names.append(unique_name)

        return output_names

    def _get_view_sql_for_dataset_by_parser(self, table_name, parser, user):
        cast = []
        plain = []
        base = []

        base = parser.column_names()
        base.append('clean')
        all_unique = parser.make_unique_columns(base)
        all_unique = self._make_safe_column_name_list(all_unique)

        for c in all_unique[0:-1]:
            cast.append("CAST(%s AS TEXT) AS %s" % (c, c))
            plain.append("%s" % c)
            base.append(c)

        clean_col = all_unique[-1]

        cast.append("1 as %s" % (clean_col))
        plain.append("0 as %s" % (clean_col))

        cast_columns = "\n     , ".join(cast)
        plain_columns = "\n     , ".join(plain)

        args = (cast_columns, user.schema, table_name,
                plain_columns, user.schema, table_name)

        return ("SELECT %s\n  FROM %s.\"%s\"\nUNION ALL\n"
                "SELECT %s\n  FROM %s.\"untyped_%s\"") % args

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
            drop_sql = self._drop_table_sql(user.schema, table_name)
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
            drop_sql = self._drop_table_sql(user.schema,
                                            "untyped_%s" % table_name)
            self.run_query(drop_sql, user, return_cursor=True).close()
            self.run_query(sql, user, return_cursor=True).close()

    def _create_table_sql(self, user, table_name, column_names, column_types):
        def _column_sql(name, col_type):
            if "int" == col_type["type"]:
                return "%s bigint" % name
            if "float" == col_type["type"]:
                return "%s decimal" % name
            if "text" == col_type["type"] and col_type["max"] > 0:
                return "%s text" % (name)
            # Fallback to text is hopefully good?
            return "%s text" % name

        columns = []
        for i in range(0, len(column_names)):
            columns.append(_column_sql(column_names[i], column_types[i]))

        sval = 'CREATE TABLE %s."%s" (%s)' % (
                    user.schema,
                    table_name,
                    ", ".join(columns)
               )
        return sval

    def _create_untyped_table_sql(self, user, table_name, names, types):
        columns = []
        for i in range(0, len(names)):
            name = names[i]
            columns.append("%s text" % name)

        return 'CREATE TABLE %s."untyped_%s" (%s)' % (
                    user.schema,
                    table_name,
                    ", ".join(columns)
               )

    def get_db_username(self, user):
        base_name = "ss_user_%s" % user
        base_name = re.sub('@', '__', base_name)
        base_name = re.sub('\.', '__', base_name)
        return base_name

    def get_db_schema(self, user):
        # stripped down schema name - prevent quoting issues
        return re.sub('[^a-zA-Z0-9]', '_', user)

    def _create_user_connection(self, user):
        username = user.db_username
        password = user.db_password
        schema = user.schema

        host = settings.DATABASES['default']['HOST']
        port = settings.DATABASES['default']['PORT']
        db = settings.DATABASES['default']['NAME']

        kwargs = {
            "user": username,
            "password": password,
            "dbname": db,
        }

        if host:
            kwargs["host"] = host

        if port:
            kwargs["port"] = port

        import psycopg2
        conn = psycopg2.connect(**kwargs)
        conn.set_session(autocommit=True)

        return conn

    def _disconnect_connection(self, connection):
        connection["connection"].close()

    def get_running_queries(self):
        query = "SELECT query FROM pg_stat_activity WHERE state='active'"

        cursor = connection.cursor()
        cursor.execute(query)

        queries = []
        row = cursor.fetchone()
        while row:
            queries.append({"sql": row[0]})
            row = cursor.fetchone()

        return queries

    def kill_query(self, query):
        try:
            connection = self.get_connection_for_user(query.owner)
            cursor = connection.cursor()
            cursor.execute("SELECT pg_cancel_backend(%s)",
                           [query.backend_terminate_data])
        except Exception as ex:
            logger.info("The errror: %s " % str(ex))
