from sqlshare_rest.models import User
import os
import random
import string


class DBInterface(object):
    def __init__(self):
        self.username = None
        self.user_connection = None

    def run_query(self, sql, user):
        self._not_implemented("run_query")

    def create_db_user(self, username, password):
        self._not_implemented("create_db_user")

    def create_db_schema(self, username, schema):
        self._not_implemented("create_db_schema")

    def remove_db_user(self, db_username):
        self._not_implemented("remove_db_user")

    def remove_schema(self, schema):
        self._not_implemented("remove_schema")

    def remove_user(self, username):
        model = User.objects.get(username=username)
        self.remove_db_user(model.db_username)
        self.remove_schema(model.schema)

    def _not_implemented(self, message):
        raise NotImplementedError("%s not implemented in %s" % (message, self))

    def get_user(self, user):
        model = None
        try:
            model = User.objects.get(username=user)
        except User.DoesNotExist:
            self._create_user(user)
            model = User.objects.get(username=user)

        return model

    def _create_user(self, user):
        username = self.get_db_username(user)
        password = self.create_db_user_password()
        schema_name = self.get_db_schema(user)

        self.create_db_user(username, password)
        self.create_db_schema(username, schema_name)

        User.objects.create(username=user,
                            db_username=username,
                            db_password=password,
                            schema=schema_name)

    # Overridable by db implementations - default comes from the C# version
    def get_db_username(self, user):
        return "meta_%s" % user

    # Just their username by default?
    def get_db_schema(self, user):
        return user

    def create_db_user_password(self):
        chars = string.ascii_letters + string.digits
        return ''.join(random.choice(chars) for i in range(40))
