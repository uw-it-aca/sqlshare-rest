from sqlshare_rest.backend.base import DBInterface
from sqlshare_rest.models import User
from django.db import connection
from django.conf import settings
import re
import hashlib

# Basic permissions to everything
# grant all on *.* to <user>
# Ability to give new users permission to their database
# grant grant option on *.* to <user>


class MySQLBackend(DBInterface):
    def create_db_user(self, username, password):
        cursor = connection.cursor()
        cursor.execute("CREATE USER %s IDENTIFIED BY %s", (username, password))

    def get_db_username(self, user):
        # MySQL only allows 16 character names.  Take the md5sum of the
        # username, and hope it's unique enough.
        hash_val = hashlib.md5(user.encode("utf-8")).hexdigest()[:11]
        test_value = "meta_%s" % (hash_val)

        try:
            existing = User.objects.get(db_username=test_value)
            msg = "Hashed DB Username already exists! " \
                  "Existing: %s, New: %s" % (exists.username, user)
            raise Exception(msg)

        except User.DoesNotExist:
            # Perfect!
            pass

        return test_value

    def get_db_schema(self, user):
        # stripped down schema name - prevent quoting issues
        return re.sub('[^a-zA-Z0-9]', '_', user)

    # Maybe this could become separate files at some point?
    def create_db_schema(self, username, schema):
        cursor = connection.cursor()
        # MySQL doesn't allow placeholders on the db name here.
        # This is protected by the get_db_schema method, which only allows
        # a-z, 0-9, and _.
        cursor.execute("CREATE DATABASE %s" % schema)

        # Using placeholders here results in bad syntax
        cursor.execute("GRANT ALL on %s.* to %s" % (schema, username))
        cursor.execute("GRANT GRANT OPTION ON %s.* TO %s" % (schema, username))

    def remove_db_user(self, user):
        cursor = connection.cursor()
        # MySQL doesn't let the username be a placeholder in DROP USER.
        cursor.execute("DROP USER %s" % (user))
        return

    def remove_schema(self, schema):
        cursor = connection.cursor()
        # MySQL doesn't allow placeholders on the db name here.
        # This is protected by the get_db_schema method, which only allows
        # a-z, 0-9, and _.
        schema = self.get_db_schema(schema)
        cursor.execute("DROP DATABASE %s" % schema)

    def delete_table(self, table, owner):
        sql = "DROP TABLE `%s`" % (table)
        self.run_query(sql, owner)

    def delete_dataset(self, dataset_name, owner):
        sql = "DROP VIEW `%s`" % (dataset_name)
        self.run_query(sql, owner)

    def get_qualified_name(self, dataset):
        return "`%s`.`%s`" % (dataset.owner.schema, dataset.name)

    def get_download_sql_for_dataset(self, dataset):
        return "SELECT * FROM %s" % self.get_qualified_name(dataset)

    def get_preview_sql_for_dataset(self, dataset_name, user):
        return "SELECT * FROM `%s`.`%s` LIMIT 100" % (user.schema,
                                                      dataset_name)

    def get_preview_sql_for_query(self, sql):
        return "SELECT * FROM (%s) as x LIMIT 100" % sql

    def _add_read_access_sql(self, dataset, owner, reader):
        return "GRANT SELECT ON `%s`.`%s` TO `%s`" % (owner.schema,
                                                      dataset,
                                                      reader.db_username)

    def _read_access_to_query_sql(self, query_id, user):
        db = self.get_query_cache_db_name()
        return "GRANT SELECT ON `%s`.`query_%s` TO `%s`" % (db,
                                                            query_id,
                                                            user.db_username)

    def delete_query(self, query_id):
        db = self.get_query_cache_db_name()
        sql = "DROP TABLE `%s`.`query_%s`" % (db, query_id)
        cursor = connection.cursor()
        cursor.execute(sql)

    def add_owner_read_access_to_query(self, query_id, user):
        return self.add_read_access_to_query(query_id, user)

    def add_read_access_to_query(self, query_id, user):
        sql = self._read_access_to_query_sql(query_id, user)
        cursor = connection.cursor()
        cursor.execute(sql)

    def add_read_access_to_dataset(self, dataset, owner, reader):
        sql = self._add_read_access_sql(dataset, owner, reader)
        self.run_query(sql, owner)

    def _remove_read_access_sql(self, dataset, owner, reader):
        db_user = reader.db_username
        return "REVOKE ALL PRIVILEGES ON `%s`.`%s` FROM `%s`" % (owner.schema,
                                                                 dataset,
                                                                 db_user)

    def remove_access_to_dataset(self, dataset, owner, reader):
        sql = self._remove_read_access_sql(dataset, owner, reader)
        self.run_query(sql, owner)

    def _add_public_access_sql(self, dataset, owner):
        public_user = self.get_public_user()
        return "GRANT SELECT ON `%s`.`%s` TO `%s`" % (owner.schema,
                                                      dataset,
                                                      public_user.db_username)

    def add_public_access(self, dataset, owner):
        return
        # Dropping "public" access, since mysql doesn't have a public role
        # sql = self._add_public_access_sql(dataset, owner)
        # self.run_query(sql, owner)

    def _remove_public_access_sql(self, dataset, owner):
        public_user = self.get_public_user()
        return "REVOKE ALL ON `%s`.`%s` FROM `%s`" % (owner.schema,
                                                      dataset,
                                                      public_user.db_username)

    def remove_public_access(self, dataset, owner):
        return
        # sql = self._remove_public_access_sql(dataset, owner)
        # self.run_query(sql, owner)

    def get_query_cache_db_name(self):
        return getattr(settings, "SQLSHARE_QUERY_CACHE_DB", "ss_query_db")

    def create_table_from_query_result(self, name, source_cursor):
        # Make sure the db exists to stash query results into
        QUERY_SCHEMA = self.get_query_cache_db_name()
        cursor = connection.cursor()
        cursor.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                       "WHERE SCHEMA_NAME = '%s'" % (QUERY_SCHEMA))

        if not cursor.rowcount:
            cursor.execute("CREATE DATABASE %s" % (QUERY_SCHEMA))

        column_def = self._get_column_definitions_for_cursor(source_cursor)

        full_name = "`%s`.`%s`" % (QUERY_SCHEMA, name)
        create_table = "CREATE TABLE %s (%s)" % (full_name, column_def)
        cursor.execute(create_table)

        row = source_cursor.fetchone()

        placeholders = ", ".join(list(map(lambda x: "%s", row)))
        insert = "INSERT INTO %s VALUES (%s)" % (full_name, placeholders)
        row_count = 0
        while row:
            cursor.execute(insert, row)
            row = source_cursor.fetchone()
            row_count += 1
        return row_count

    def get_query_sample_sql(self, query_id):
        QUERY_SCHEMA = self.get_query_cache_db_name()
        return "SELECT * FROM %s.query_%s LIMIT 100" % (QUERY_SCHEMA, query_id)

    def _create_snapshot_table(self, source_dataset, table_name, user):
        sql = "CREATE TABLE `%s` AS SELECT * FROM %s" % (table_name,
                                                         source_dataset.name)

        self.run_query(sql, user)

    def _get_snapshot_view_sql(self, dataset):
        table_name = self._get_table_name_for_dataset(dataset.name)
        return ("CREATE OR REPLACE VIEW `%s` AS "
                "SELECT * FROM `%s`.`%s`" % (dataset.name,
                                             dataset.owner.schema,
                                             table_name))

    def _get_column_definitions_for_cursor(self, cursor):
        import pymysql
        # XXX - is defining this a sign that this is a mistake?
        FLOAT = pymysql.DBAPISet([pymysql.FIELD_TYPE.DECIMAL,
                                  pymysql.FIELD_TYPE.NEWDECIMAL,
                                  pymysql.FIELD_TYPE.DOUBLE,
                                  pymysql.FIELD_TYPE.FLOAT])

        index = 0
        column_defs = []
        for col in cursor.description:
            index = index + 1
            col_type = col[1]
            col_len = col[3]
            null_ok = col[6]

            column_name = "COLUMN%s" % index

            if col_type == FLOAT:
                if null_ok:
                    column_defs.append("%s FLOAT" % column_name)
                else:
                    column_defs.append("%s FLOAT NOT NULL" % column_name)
            elif col_type == pymysql.NUMBER:
                if null_ok:
                    column_defs.append("%s INT" % column_name)
                else:
                    column_defs.append("%s INT NOT NULL" % column_name)

            elif col_type == pymysql.STRING and col_len:
                if null_ok:
                    column_defs.append("%s VARCHAR(%s)" % (column_name,
                                                           col_len))
                else:
                    base_str = "%s VARCHAR(%s) NOT NULL"
                    column_defs.append(base_str % (column_name, col_len))
            else:
                column_defs.append("%s TEXT" % column_name)

        return ", ".join(column_defs)

    def _create_table(self, table_name, column_names, column_types, user):
        try:
            sql = self._create_table_sql(table_name,
                                         column_names,
                                         column_types)
            self.run_query(sql, user)
        except:
            drop_sql = self._drop_exisisting_table_sql(table_name)
            self.run_query(drop_sql, user)
            self.run_query(sql, user)

    def _drop_exisisting_table_sql(self, table_name):
        return "DROP TABLE `%s`" % (table_name)

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

        return "CREATE TABLE `%s` (%s) ENGINE InnoDB CHARACTER SET utf8 " \
               "COLLATE utf8_bin" % (
                    table_name,
                    ", ".join(columns)
               )

    def _load_table_sql(self, table_name, row, user):
        placeholders = map(lambda x: "%s", row)
        return "INSERT INTO `%s`.`%s` VALUES (%s)" % (user.schema, table_name,
                                                      ", ".join(placeholders))

    def _load_table(self, table_name, data_handle, upload, user):
        for row in data_handle:
            sql = self._load_table_sql(table_name, row, user)
            self.run_query(sql, user, row)

    def _disconnect_connection(self, connection):
        connection["connection"].close()

    def create_view(self, name, sql, user):
        view_sql = self._create_view_sql(name, sql)
        self.run_query(view_sql, user)

        count_sql = "SELECT COUNT(*) FROM `%s`" % (name)
        result = self.run_query(count_sql, user)
        return result[0][0]

    def _create_view_sql(self, name, sql):
        return "CREATE OR REPLACE VIEW `%s` AS %s" % (name, sql)

    def _get_view_sql_for_dataset(self, table_name, user):
        return "SELECT * FROM `%s`.`%s`" % (user.schema, table_name)

    def run_query(self, sql, user, params=None, return_cursor=False):
        connection = self.get_connection_for_user(user)
        cursor = connection.cursor()
        cursor.execute(sql, params)

        if return_cursor:
            return cursor
        return cursor.fetchall()

    def _create_user_connection(self, user):
        username = user.db_username
        password = user.db_password
        schema = user.schema

        host = settings.DATABASES['default']['HOST']
        port = settings.DATABASES['default']['PORT']

        kwargs = {
            "user": username,
            "passwd": password,
            "db": schema,
            "autocommit": True,
        }

        if host:
            kwargs["host"] = host

        if port:
            kwargs["port"] = port

        import pymysql
        conn = pymysql.connect(**kwargs)

        return conn

    def get_running_queries(self):
        query = "SHOW FULL PROCESSLIST"

        cursor = connection.cursor()
        cursor.execute(query)

        queries = []
        row = cursor.fetchone()
        while row:
            queries.append({"sql": row[7]})
            row = cursor.fetchone()

        return queries
