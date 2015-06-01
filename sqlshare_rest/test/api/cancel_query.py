from django.test import TestCase
from unittest2 import skipIf
from django.db import connection
from time import sleep
from multiprocessing import Process
import json
import re
from sqlshare_rest.util.db import get_backend, is_mssql
from sqlshare_rest.dao.query import create_query
from sqlshare_rest.test import missing_url
from django.test.utils import override_settings
from django.test.client import Client
from django.core.urlresolvers import reverse
from sqlshare_rest.test.api.base import BaseAPITest
from sqlshare_rest.dao.dataset import create_dataset_from_query
from sqlshare_rest.util.query_queue import process_queue
from sqlshare_rest.models import Query

@skipIf(missing_url("sqlshare_view_dataset_list"), "SQLShare REST URLs not configured")
@override_settings(MIDDLEWARE_CLASSES = (
                                'django.contrib.sessions.middleware.SessionMiddleware',
                                'django.middleware.common.CommonMiddleware',
                                'django.contrib.auth.middleware.AuthenticationMiddleware',
                                'django.contrib.auth.middleware.RemoteUserMiddleware',
                                'django.contrib.messages.middleware.MessageMiddleware',
                                'django.middleware.clickjacking.XFrameOptionsMiddleware',
                                ),
                   AUTHENTICATION_BACKENDS = ('django.contrib.auth.backends.ModelBackend',)
                   )

class CancelQueryAPITest(BaseAPITest):
    def setUp(self):
        # Try to cleanup from any previous test runs...
        self.remove_users = []
        self.client = Client()

    def test_cancel(self):
        owner = "cancel_user1"
        not_owner = "cancel_user2"
        self.remove_users.append(owner)
        self.remove_users.append(not_owner)
        backend = get_backend()
        user = backend.get_user(owner)

        Query.objects.all().delete()

        query_text = None
        if is_mssql():
            query_text = "select (22) waitfor delay '00:10:30'"

        def queue_runner():
            from django import db
            db.close_connection()
            process_queue(verbose=False, thread_count=2, run_once=False)


        from django import db
        db.close_connection()
        p = Process(target=queue_runner)
        p.start()
        # We need to have the server up and running before creating the query...
        sleep(2)
        query = create_query(owner, query_text)
        query_id = query.pk

        # This just needs to wait for the process to start.  1 wasn't reliable,
        # 2 seemed to be.  If this isn't, maybe turn this into a loop waiting
        # for the query to show up?
        sleep(3)

        try:
            queries = backend.get_running_queries()

            has_query = False
            for q in queries:
                if q["sql"] == query_text:
                    has_query = True

            self.assertTrue(has_query)

            auth_headers = self.get_auth_header_for_username(owner)
            bad_auth_headers = self.get_auth_header_for_username(not_owner)

            url = reverse("sqlshare_view_query", kwargs={ "id": query.pk })
            response = self.client.delete(url, **bad_auth_headers)

            has_query = False
            queries = backend.get_running_queries()
            for q in queries:
                if q["sql"] == query_text:
                    has_query = True

            self.assertTrue(has_query)

            url = reverse("sqlshare_view_query", kwargs={ "id": query.pk })
            response = self.client.delete(url, **auth_headers)
            sleep(1)

            has_query = False
            queries = backend.get_running_queries()
            for q in queries:
                if q["sql"] == query_text:
                    has_query = True

            self.assertFalse(has_query)

            q2 = Query.objects.get(pk = query_id)
            self.assertTrue(q2.is_finished)
            self.assertTrue(q2.has_error)
            self.assertTrue(q2.terminated)
            self.assertEquals(q2.error, "Query cancelled")
        except Exception as ex:
            raise
        finally:
            p.terminate()
            p.join()


        Query.objects.all().delete()

        q2 = create_query(owner, query_text)
        url = reverse("sqlshare_view_query", kwargs={ "id": q2.pk })
        response = self.client.delete(url, **auth_headers)

        q2 = Query.objects.get(pk = q2.pk)
        self.assertFalse(q2.is_finished)
        self.assertFalse(q2.has_error)
        self.assertTrue(q2.terminated)
        process_queue(run_once=True, verbose=True)
        q2 = Query.objects.get(pk = q2.pk)
        self.assertTrue(q2.is_finished)
        self.assertTrue(q2.has_error)
        self.assertTrue(q2.terminated)


    @classmethod
    def setUpClass(cls):
        def _run_query(sql):
            cursor = connection.cursor()
            try:
                cursor.execute(sql)
            except Exception as ex:
                # Hopefully all of these will fail, so ignore the failures
                pass

        # This is just an embarrassing list of things to cleanup if something fails.
        # It gets added to when something like this blocks one of my test runs...
        _run_query("drop login cancel_user1")
