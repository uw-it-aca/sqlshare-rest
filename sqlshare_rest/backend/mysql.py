from sqlshare_rest.backend.base import DBInterface
from sqlshare_rest.models import User
from django.db import connection
import re
import hashlib

# grant create user on *.* to <user>
# grant create on *.* to <user>
# grant drop on *.* to <user>


class MySQLBackend(DBInterface):
    def create_db_user(self, username, password):
        cursor = connection.cursor()
        cursor.execute("CREATE USER %s IDENTIFIED BY %s", (username, password))
        return

    def get_db_username(self, user):
        # MySQL only allows 16 character names.  Take the md5sum of the
        # username, and hope it's unique enough.
        test_value = "meta_%s" % (hashlib.md5(user).hexdigest()[:11])

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

    def run_query(self, sql, username):
        cursor = connection.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
