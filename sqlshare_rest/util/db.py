from django.conf import settings
from sqlshare_rest.backend.pg import PGBackend
from sqlshare_rest.backend.mysql import MySQLBackend
from sqlshare_rest.backend.sqlite3 import SQLite3Backend
from sqlshare_rest.backend.mssql import MSSQLBackend, SQLAzureBackend


def _get_basic_settings():
    return settings.DATABASES['default']


def get_backend():
    engine = _get_basic_settings()['ENGINE']

    if is_pg():
        return PGBackend()
    elif is_mysql():
        return MySQLBackend()
    elif is_sqlite3():
        return SQLite3Backend()
    elif is_sql_azure():
        return SQLAzureBackend()
    elif is_mssql():
        return MSSQLBackend()
    else:
        raise BackendNotImplemented(engine)
    pass


def is_pg():
    pg_backend = "django.db.backends.postgresql_psycopg2"
    return _get_basic_settings()['ENGINE'] == pg_backend


def is_mysql():
    return _get_basic_settings()['ENGINE'] == "django.db.backends.mysql"


def is_sqlite3():
    return _get_basic_settings()['ENGINE'] == "django.db.backends.sqlite3"


def is_mssql():
    return (_get_basic_settings()['ENGINE'] == "django_pyodbc" and
            not is_sql_azure())


def is_sql_azure():
    return (_get_basic_settings()['ENGINE'] == "django_pyodbc" and
            getattr(settings, "SQLSHARE_IS_AZURE", False))


class BackendNotImplemented(Exception):
    pass
