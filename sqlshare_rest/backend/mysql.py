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

    def _disconnect_connection(self, connection):
        connection["connection"].close()

    def create_view(self, name, sql, user):
        view_sql = "CREATE OR REPLACE VIEW %s AS %s" % (name, sql)
        self.run_query(view_sql, user)
        return

    def run_query(self, sql, user):
        connection = self.get_connection_for_user(user)
        cursor = connection.cursor()
        cursor.execute(sql)
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
