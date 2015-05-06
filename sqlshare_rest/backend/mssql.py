from sqlshare_rest.backend.base import DBInterface
from sqlshare_rest.models import User
from django import db
from django.db import connection
from django.conf import settings
from contextlib import closing
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
            cursor.execute("CREATE SCHEMA %s" % (schema))
            cursor.execute("GRANT CONNECT TO %s" % (username))

    def remove_db_user(self, user):
        with closing(connection.cursor()) as cursor:
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

    def create_view(self, name, sql, user):
        # view_sql = "CREATE OR REPLACE VIEW %s AS %s" % (name, sql)
        # self.run_query(view_sql, user)
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
        # return get_new_connection(**args)
        # username = user.db_username
        # password = user.db_password
        # schema = user.schema
        #
        # host = settings.DATABASES['default']['HOST']
        # port = settings.DATABASES['default']['PORT']
        #
        # kwargs = {
        #     "user": username,
        #     "passwd": password,
        #     "db": schema,
        # }
        #
        # if host:
        #     kwargs["host"] = host
        #
        # if port:
        #     kwargs["port"] = port
        #
        # import pymysql
        # conn = pymysql.connect(**kwargs)
        #
        # return conn
