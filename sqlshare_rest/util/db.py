from django.conf import settings
from sqlshare_rest.backend.mysql import MySQLBackend

def _get_basic_settings():
    return settings.DATABASES['default']

def _get_backend():
    engine = _get_basic_settings()['ENGINE']

    if engine == "django.db.backends.mysql":
        return MySQLBackend()
    else:
        raise BackendNotImplemented(engine)
    pass


class BackendNotImplemented(Exception):
    pass

