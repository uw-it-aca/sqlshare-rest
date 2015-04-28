from django.conf import settings
from sqlshare_rest.backend.mysql import MySQLBackend
from sqlshare_rest.backend.sqlite3 import SQLite3Backend
from sqlshare_rest.backend.mssql import MSSQLBackend


def _get_basic_settings():
    return settings.DATABASES['default']


def get_backend():
    engine = _get_basic_settings()['ENGINE']

    if is_mysql():
        return MySQLBackend()
    if is_sqlite3():
        return SQLite3Backend()
    if is_mssql():
        return MSSQLBackend
    else:
        raise BackendNotImplemented(engine)
    pass


def is_mysql():
    return _get_basic_settings()['ENGINE'] == "django.db.backends.mysql"


def is_sqlite3():
    return _get_basic_settings()['ENGINE'] == "django.db.backends.sqlite3"


def is_mssql():
    return _get_basic_settings()['ENGINE'] == "sqlserver_ado"


class BackendNotImplemented(Exception):
    pass
