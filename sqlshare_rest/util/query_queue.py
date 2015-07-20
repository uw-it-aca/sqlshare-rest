from sqlshare_rest.util.db import get_backend
from sqlshare_rest.models import Query
from sqlshare_rest.dao.dataset import reset_dataset_account_access
from django.utils import timezone
from django.conf import settings
from time import sleep
from sqlshare_rest.util.queue_triggers import trigger_query_queue_processing
from sqlshare_rest.util.queue_triggers import QUERY_QUEUE_PORT_NUMBER
from sqlshare_rest.logger import getLogger
import atexit
import signal
import json
import time
import sys
import os

import socket
from threading import Thread

import six
TERMINATE_TRIGGER_FILE = getattr(settings,
                                 "SQLSHARE_TERMINATE_QUERY_QUEUE_PATH",
                                 "/tmp/sqlshare_terminate_query_queue")


def process_queue(thread_count=0, run_once=True, verbose=False):
    def start_query(query, background=True):
        query.is_started = True
        query.save()
        query_id = query.pk

        if background:
            from django.db import connection
            connection.close()

            if os.fork():
                # This is the main process
                return

            os.setsid()

            if os.fork():
                # Double fork the daemon
                sys.exit(0)

            # Close stdin/out/err
            sys.stdin.flush()
            sys.stdout.flush()
            sys.stderr.flush()
            null = os.open(os.devnull, os.O_RDWR)
            os.dup2(null, sys.stdin.fileno())
            os.dup2(null, sys.stdout.fileno())
            os.dup2(null, sys.stderr.fileno())
            os.close(null)

        try:
            process_query(query_id)
        except Exception as ex:
            try:
                query = Query.objects.get(pk=query_id)
                query.has_error = True
                query.error = str(ex)
                query.is_finished = True
                query.save()
            except:
                # That try is just trying to get info out to the user, it's
                # relatively ok if that fails
                pass
            logger = getLogger(__name__)
            logger.error("Error on %s: %s" % (query_id, str(ex)))

        if background:
            sys.exit(0)

    def get_column_names_from_cursor(cursor):
        index = 0
        names = []
        for col in cursor.description:
            index += 1
            column_name = col[0]
            if column_name == "":
                column_name = "COLUMN%s" % index

            names.append(column_name)

        return names

    def process_query(query_id):
        logger = getLogger(__name__)
        query = Query.objects.get(pk=query_id)
        # queries can be cancelled before we see them.  clean it up now.

        if query.terminated:
            query.is_finished = True
            query.has_error = True
            query.error = "Query cancelled"
            query.save()
            return

        pid = os.getpid()
        query.process_queue_id = pid
        query.save()

        msg = "Processing query id %s, in process %s" % (
            query.pk,
            pid
        )
        logger.info(msg)
        if verbose:
            print(msg)
        user = query.owner
        row_count = 0
        backend = get_backend()
        try:
            start = timezone.now()
            query_plan = backend.get_query_plan(query.sql, user)

            t1 = time.time()
            cursor = backend.run_query(query.sql,
                                       user,
                                       return_cursor=True)

            t2 = time.time()
            try:
                all_data = []
                for row in cursor:
                    all_data.append(list(row))
                    row_count += 1

                columns = get_column_names_from_cursor(cursor)
                formatted = json.dumps({"columns": columns, "data": all_data})
                query.preview_content = formatted
                t3 = time.time()

                query.query_time = t2-t1
                query.total_time = t3-t1
                query.query_plan = query_plan
                query.save()

                end = timezone.now()
            except:
                raise
        except Exception as ex:
            msg = "Error running query %s: %s" % (query.pk,
                                                  str(ex))
            logger.error(msg)
            query.has_error = True
            query.error = str(ex)
        finally:
            backend.close_user_connection(user)

        try:
            query.is_finished = True
            query.date_finished = timezone.now()
            query.rows_total = row_count
            query.save()

            if query.is_preview_for:
                dataset = query.is_preview_for
                dataset.preview_is_finished = True
                dataset.preview_error = query.error
                # Make sure all current users can see the preview table
                reset_dataset_account_access(dataset)
                dataset.save()
        except Exception as ex:
            logger.error("Error: %s" % str(ex))

        msg = "Finished query id %s." % query.pk
        logger.info(msg)

    def periodic_check():
        """
        Every 5 seconds, do a check for new queries.  Just in case something
        needs processing, but didn't call trigger_processing() itself.
        """
        logger = getLogger(__name__)
        while True:
            sleep(5)
            msg = "Triggering periodic processing."
            logger.debug(msg)
            if verbose:
                print(msg)
            trigger_query_queue_processing()

    filtered = Query.objects.filter(is_started=False)
    if run_once:
        try:
            oldest_query = filtered.order_by('id')[:1].get()
        except Query.DoesNotExist:
            return

        start_query(oldest_query, background=False)

    else:
        # Start with any queries already in the queue:
        for query in filtered:
            start_query(query)

        # Just in case things get off the rails - maybe a connection to the
        # server gets blocked? - periodically trigger a check for new queries
        kicker = Thread(target=periodic_check)
        kicker.setDaemon(True)
        kicker.start()

        # Start the socket server for getting notifications of new queries
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Make it so we can run the server right away after killing it
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('localhost', QUERY_QUEUE_PORT_NUMBER))

        # Make sure we close our socket when we're killed.
        def close_socket():
            server.close()

        atexit.register(close_socket)

        def kill_query(query):
            logger = getLogger(__name__)
            pid = query.process_queue_id
            query.is_started = True
            query.is_finished = True
            query.has_error = True
            query.error = "Query cancelled"
            query.save()

            logger.info("Cancelling query: %s" % query.pk)
            os.kill(pid, signal.SIGKILL)

        server.listen(5)
        while True:
            (clientsocket, address) = server.accept()
            # Check to see if we should exit...
            if os.path.isfile(TERMINATE_TRIGGER_FILE):
                sys.exit(0)
            # We don't actually have a protocol to speak...
            clientsocket.close()
            terminate_list = Query.objects.filter(terminated=True,
                                                  is_finished=False)

            for query in terminate_list:
                kill_query(query)

            queries = Query.objects.filter(is_started=False)
            for query in queries:
                start_query(query)


def kill_query_queue():
    # Create the file that triggers the termination
    f = open(TERMINATE_TRIGGER_FILE, "w")
    f.write("OK")
    f.close()

    # Trigger the check...
    trigger_query_queue_processing()

    # Just a quick pause before polling
    time.sleep(0.3)

    # Poll to see if the process is still running...
    for i in range(10):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(('localhost', QUERY_QUEUE_PORT_NUMBER))
            time.sleep(1)
        except socket.error as ex:
            os.remove(TERMINATE_TRIGGER_FILE)
            return True

    os.remove(TERMINATE_TRIGGER_FILE)
    return False
