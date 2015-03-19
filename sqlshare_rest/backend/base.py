from sqlshare_rest.models import User
import os
import random
import string


class DBInterface(object):

    USER_CONNECTIONS = {}

    def __init__(self):
        self.username = None

    def run_query(self, sql, user):
        self._not_implemented("run_query")

    def create_view(self, name, sql, user):
        self._not_implemented("create_view")

    def create_db_user(self, username, password):
        self._not_implemented("create_db_user")

    def create_db_schema(self, username, schema):
        self._not_implemented("create_db_schema")

    def create_snapshot(self, source_dataset, destination_datset, user):
        self._not_implemented("create_snapshot")

    def remove_db_user(self, db_username):
        self._not_implemented("remove_db_user")

    def remove_schema(self, schema):
        self._not_implemented("remove_schema")

    def remove_user(self, username):
        model = User.objects.get(username=username)
        self.remove_db_user(model.db_username)
        self.remove_schema(model.schema)

    def add_read_access_to_dataset(dataset, owner, reader):
        self._not_implemented("add_read_access_to_dataset")

    def remove_access_to_dataset(dataset, owner, reader):
        self._not_implemented("remove_access_to_dataset")

    def create_dataset_from_parser(self, dataset_name, parser, user):
        """
        Turns a parser object into a dataset.
        # Creates a table based on the parser columns
        # Loads the data that's in the handle for the parser
        # Creates the view for the dataset
        """
        table_name = self._get_table_name_for_dataset(dataset_name)
        self._create_table(table_name=table_name,
                           column_names=parser.column_names(),
                           column_types=parser.column_types(),
                           user=user)

        self._load_table(table_name, parser.get_data_handle(), user)
        self.create_view(dataset_name,
                         self._get_view_sql_for_dataset(table_name, user),
                         user)

    def _get_view_sql_for_dataset(self, table_name, user):
        """
        The SQL statement that creates a view of the given table of data
        """
        self._not_implemented("_get_view_sql_for_dataset")

    def _create_user_connection(self, user):
        """
        Builds a per-user connection to the database
        """
        self._not_implemented("_create_user_connection")

    def _disconnect_connection(self, connection):
        """
        Disconnects a per-user connection to the database
        """
        self._not_implemented("_disconnect_connection")

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

    def _load_table(self, table_name, data_handle, user):
        """
        Add data from data_handle to the table.  data_handle must be iterable,
        and the type values for columns must be correct.
        """
        raise NotImplementedError("_load_table")

    def _create_table(self, table_name, column_names, column_types, user):
        """
        Create a table, building the definition from the column names and
        types given.
        """
        raise NotImplementedError("_create_table")

    # Overridable - default comes from the C# version
    def _get_table_name_for_dataset(self, dataset_name):
        return "table_%s" % (dataset_name)

    # Overridable by db implementations - default comes from the C# version
    def get_db_username(self, user):
        return "meta_%s" % user

    # Just their username by default?
    def get_db_schema(self, user):
        return user

    def create_db_user_password(self):
        chars = string.ascii_letters + string.digits
        return ''.join(random.choice(chars) for i in range(40))

    def get_connection_for_user(self, user):
        by_user = DBInterface.USER_CONNECTIONS
        if user.db_username not in by_user:
            connection = self._create_user_connection(user)
            by_user[user.db_username] = {"connection": connection,
                                         "user": user}
        return by_user[user.db_username]["connection"]

    def close_user_connection(self, user):
        by_user = DBInterface.USER_CONNECTIONS
        if user.db_username in by_user:
            self._disconnect_connection(by_user[user.db_username])
            del by_user[user.db_username]
