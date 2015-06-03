from django.test import TestCase, RequestFactory
from django.contrib.auth.models import User
from unittest2 import skipIf
from django.db import connection
import json
import re
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.test import missing_url
from django.test.utils import override_settings
from django.test.client import Client
from django.core.urlresolvers import reverse
from sqlshare_rest.test.api.base import BaseAPITest
from sqlshare_rest.dao.dataset import create_dataset_from_query
from testfixtures import LogCapture
from sqlshare_rest.logger import getLogger
from sqlshare_rest.dao.user import set_override_user


@skipIf(missing_url("sqlshare_view_dataset_list"), "SQLShare REST URLs not configured")
@override_settings(MIDDLEWARE_CLASSES = (
                                'django.contrib.sessions.middleware.SessionMiddleware',
                                'django.middleware.common.CommonMiddleware',
                                'django.contrib.auth.middleware.AuthenticationMiddleware',
                                'django.contrib.auth.middleware.RemoteUserMiddleware',
                                'django.contrib.messages.middleware.MessageMiddleware',
                                'django.middleware.clickjacking.XFrameOptionsMiddleware',
                                ),
                   AUTHENTICATION_BACKENDS = ('django.contrib.auth.backends.ModelBackend',),
                  )


class TestLogging(BaseAPITest):
    def setUp(self):
        self.remove_users = []
        self.client = Client()

    def _has_log(self, l, user, override, name, level, message):
        has_log = False
        acting = user
        if override:
            acting = override

        formatted = "Actual: %s; Acting: %s; %s" % (user, acting, message)
        if not user:
            formatted = "Offline; %s" % (message)

        for log in l.actual():
            log_name = log[0]
            log_level = log[1]
            msg = log[2]

            if msg == formatted and name == log_name and level == log_level:
                has_log = True

        return has_log

    def test_interface(self):
        username1 = "log_test_user1"
        username2 = "log_test_user2"
        self.remove_users.append(username1)
        self.remove_users.append(username2)
        logger = getLogger(__name__)
        with LogCapture() as l:
            logger.debug("Debug Msg")
            self.assertTrue(self._has_log(l, None, None, "sqlshare_rest.test.logging", "DEBUG", "Debug Msg"))

        with LogCapture() as l:
            logger.warn("Warn Msg")
            self.assertTrue(self._has_log(l, None, None, "sqlshare_rest.test.logging", "WARNING", "Warn Msg"))

        with LogCapture() as l:
            logger.info("Info Msg")
            self.assertTrue(self._has_log(l, None, None, "sqlshare_rest.test.logging", "INFO", "Info Msg"))

        with LogCapture() as l:
            logger.error("Error Msg")
            self.assertTrue(self._has_log(l, None, None, "sqlshare_rest.test.logging", "ERROR", "Error Msg"))

        with LogCapture() as l:
            logger.critical("Critical Msg")
            self.assertTrue(self._has_log(l, None, None, "sqlshare_rest.test.logging", "CRITICAL", "Critical Msg"))


        request = RequestFactory().get("/")
        user = self.user = User.objects.create_user(username=username1)
        request.user = user
        with LogCapture() as l:
            logger.debug("Debug Msg", request)
            self.assertTrue(self._has_log(l, username1, None, "sqlshare_rest.test.logging", "DEBUG", "Debug Msg"))

        with LogCapture() as l:
            logger.warn("Warn Msg", request)
            self.assertTrue(self._has_log(l, username1, None, "sqlshare_rest.test.logging", "WARNING", "Warn Msg"))

        with LogCapture() as l:
            logger.info("Info Msg", request)
            self.assertTrue(self._has_log(l, username1, None, "sqlshare_rest.test.logging", "INFO", "Info Msg"))

        with LogCapture() as l:
            logger.error("Error Msg", request)
            self.assertTrue(self._has_log(l, username1, None, "sqlshare_rest.test.logging", "ERROR", "Error Msg"))

        with LogCapture() as l:
            logger.critical("Critical Msg", request)
            self.assertTrue(self._has_log(l, username1, None, "sqlshare_rest.test.logging", "CRITICAL", "Critical Msg"))

        set_override_user(request, username2)
        with LogCapture() as l:
            logger.debug("Debug Msg", request)
            self.assertTrue(self._has_log(l, username1, username2, "sqlshare_rest.test.logging", "DEBUG", "Debug Msg"))

        with LogCapture() as l:
            logger.warn("Warn Msg", request)
            self.assertTrue(self._has_log(l, username1, username2, "sqlshare_rest.test.logging", "WARNING", "Warn Msg"))

        with LogCapture() as l:
            logger.info("Info Msg", request)
            self.assertTrue(self._has_log(l, username1, username2, "sqlshare_rest.test.logging", "INFO", "Info Msg"))

        with LogCapture() as l:
            logger.error("Error Msg", request)
            self.assertTrue(self._has_log(l, username1, username2, "sqlshare_rest.test.logging", "ERROR", "Error Msg"))

        with LogCapture() as l:
            logger.critical("Critical Msg", request)
            self.assertTrue(self._has_log(l, username1, username2, "sqlshare_rest.test.logging", "CRITICAL", "Critical Msg"))

#    def test_api(self):
#        user = "log_test_user3"
#        self.remove_users.append(user)
#
#        with LogCapture() as l:
#            auth_headers = self.get_auth_header_for_username(user)
#
#            data = {
#                "sql": "select(1)"
#            }
#
#            post_url = reverse("sqlshare_view_query_list")
#            response = self.client.post(post_url, data=json.dumps(data), content_type='application/json', **auth_headers)
#
#            self.assertTrue(self._has_log(l, user, None, "sqlshare.views.query_list", "INFO", "Started query: select(1)"))
