from sqlshare_rest.backend.base import DBInterface
from sqlshare_rest.models import User
from django.db import connection
from django.conf import settings
import re


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

    def run_query(self, sql, user, params=None, return_cursor=False):
        connection = self.get_connection_for_user(user)
        cursor = connection.cursor()
        cursor.execute(sql, params)

        if return_cursor:
            return cursor
        return cursor.fetchall()

    def _create_view_sql(self, schema, name, sql):
        return "CREATE VIEW %s.%s AS %s" % (schema, name, sql)

    def _drop_view_sql(self, schema, name):
        return "DROP VIEW %s.%s CASCADE" % (schema, name)

    def delete_dataset(self, dataset_name, owner):
        sql = self._drop_view_sql(owner.schema, dataset_name)
        self.run_query(sql, owner, return_cursor=True).close()

    def create_view(self, name, sql, user):
        schema = user.schema
        view_sql = self._create_view_sql(schema, name, sql)
        try:
            self.run_query(view_sql, user, return_cursor=True).close()
        except Exception as ex:
            try:
                drop_sql = self._drop_view_sql(schema, name)
                self.run_query(drop_sql, user, return_cursor=True).close()
                self.run_query(view_sql, user, return_cursor=True).close()
            except Exception as ex:
                print "E: ", ex
                raise

        count_sql = "SELECT COUNT(*) FROM %s.%s" % (schema, name)

        result = self.run_query(count_sql, user)
        return result[0][0]

    def add_public_access(self, dataset, owner):
        sql = "GRANT SELECT ON %s.%s to PUBLIC" % (owner.schema, dataset.name)
        self.run_query(sql, owner, return_cursor=True).close()

    def remove_public_access(self, dataset, owner):
        sql = "REVOKE SELECT ON %s.%s FROM PUBLIC" % (owner.schema,
                                                      dataset.name)
        self.run_query(sql, owner, return_cursor=True).close()

    def add_read_access_to_dataset(self, dataset, owner, reader):
        sql = "GRANT SELECT ON %s.%s to %s" % (owner.schema, dataset,
                                               reader.db_username)
        self.run_query(sql, owner, return_cursor=True).close()

    def remove_access_to_dataset(self, dataset, owner, reader):
        sql = "REVOKE ALL ON %s.%s FROM %s" % (owner.schema, dataset,
                                               reader.db_username)
        self.run_query(sql, owner, return_cursor=True).close()

    def get_preview_sql_for_dataset(self, dataset_name, user):
        return "SELECT * FROM %s.%s LIMIT 100" % (user.schema, dataset_name)

    def get_view_sql_for_dataset(self, dataset):
        return "SELECT * FROM %s" % self.get_qualified_name(dataset)

    def get_download_sql_for_dataset(self, dataset):
        return "SELECT * FROM %s" % self.get_qualified_name(dataset)

    def get_qualified_name(self, dataset):
        return "%s.%s" % (dataset.owner.schema, dataset.name)

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
