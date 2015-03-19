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
        return

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

    def _create_snapshot_sql(self, source_dataset, destination_datset):
        """
        Requires the source to be quoted, the destination to not be.

        Source could be another user's dataset, so we can't quote that.
        """
        return "CREATE TABLE `%s` AS SELECT * FROM %s" % (destination_datset,
                                                          source_dataset)

    def create_snapshot(self, source_dataset, destination_datset, user):
        table_name = self._get_table_name_for_dataset(destination_datset)
        sql = self._create_snapshot_sql(source_dataset, table_name)
        self.run_query(sql, user)
        self.create_view(destination_datset,
                         self._get_view_sql_for_dataset(table_name, user),
                         user)


    def _add_read_access_sql(self, dataset, owner, reader):
        return "GRANT SELECT ON `%s`.`%s` TO `%s`" % (owner.schema,
                                                      dataset,
                                                      reader.db_username)

    def add_read_access_to_dataset(self, dataset, owner, reader):
        pass

    def _remove_read_access_sql(self, dataset, owner, reader):
        db_user = reader.db_username
        return "REVOKE ALL PRIVILEGES ON `%s`.`%s` FROM `%s`" % (owner.schema,
                                                                 dataset,
                                                                 db_user)

    def remove_access_to_dataset(self, dataset, owner, reader):
        pass

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

        return "CREATE TABLE `%s` (%s) ENGINE InnoDB CHARACTER SET utf8 " \
               "COLLATE utf8_bin" % (
                    table_name,
                    ", ".join(columns)
               )

    def _load_table_sql(self, table_name, row):
        placeholders = map(lambda x: "%s", row)
        return "INSERT INTO `%s` VALUES (%s)" % (table_name,
                                                 ", ".join(placeholders))

    def _load_table(self, table_name, data_handle, user):
        for row in data_handle:
            sql = self._load_table_sql(table_name, row)
            self.run_query(sql, user, row)

    def _disconnect_connection(self, connection):
        connection["connection"].close()

    def create_view(self, name, sql, user):
        view_sql = self._create_view_sql(name, sql)
        self.run_query(view_sql, user)
        return

    def _create_view_sql(self, name, sql):
        return "CREATE OR REPLACE VIEW `%s` AS %s" % (name, sql)

    def _get_view_sql_for_dataset(self, table_name, user):
        return "SELECT * FROM `%s`.`%s`" % (user.schema, table_name)

    def run_query(self, sql, user, params=None):
        connection = self.get_connection_for_user(user)
        cursor = connection.cursor()
        cursor.execute(sql, params)
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
        }

        if host:
            kwargs["host"] = host

        if port:
            kwargs["port"] = port

        import pymysql
        conn = pymysql.connect(**kwargs)

        return conn
