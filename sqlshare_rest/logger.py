import logging
from sqlshare_rest.dao.user import get_original_user, get_override_user


class getLogger(object):
    """
    An interface for logging that is user-override aware.
    """
    def __init__(self, name):
        self._logger = logging.getLogger(name)

    def debug(self, msg, request=None):
        self._logger.debug(self._format(msg, request))

    def info(self, msg, request=None):
        self._logger.info(self._format(msg, request))

    def warn(self, msg, request=None):
        return self.warning(msg, request)

    def warning(self, msg, request=None):
        self._logger.warning(self._format(msg, request))

    def error(self, msg, request=None):
        self._logger.error(self._format(msg, request))

    def critical(self, msg, request=None):
        self._logger.critical(self._format(msg, request))

    def _format(self, msg, request):
        if request:
            original = get_original_user(request)
            override = get_override_user(request)

            actual = original.username
            acting = actual

            if override:
                acting = override.username

            return "Actual: %s; Acting: %s; %s" % (actual, acting, msg)

        else:
            return "Offline; %s" % (msg)
