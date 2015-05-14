from sqlshare_rest.util.db import get_backend
from sqlshare_rest.models import Query
from sqlshare_rest.dao.dataset import reset_dataset_account_access
from django.utils import timezone
from time import sleep
from sqlshare_rest.util.queue_triggers import trigger_query_queue_processing
from sqlshare_rest.util.queue_triggers import QUERY_QUEUE_PORT_NUMBER
import atexit

import socket
from threading import Thread

import six
from multiprocessing import Process, Manager, Queue


def process_queue(thread_count=0, run_once=True, verbose=False):

    def worker(q):
        """
        Get a query from the queue, and process it...
        """
        backend = get_backend()
        keep_looping = True
        while keep_looping:
            oldest_query = q.get()
            if verbose:
                print("Processing query id %s." % oldest_query.pk)
            user = oldest_query.owner
            row_count = 0
            try:
                start = timezone.now()
                cursor = backend.run_query(oldest_query.sql,
                                           user,
                                           return_cursor=True)

                name = "query_%s" % oldest_query.pk
                try:
                    row_count = backend.create_table_from_query_result(name,
                                                                       cursor)
                    backend.add_owner_read_access_to_query(oldest_query.pk,
                                                           user)

                    end = timezone.now()
                except:
                    raise
            except Exception as ex:
                if verbose:
                    print("Error running query %s: %s" % (oldest_query.pk,
                                                          str(ex)))
                oldest_query.has_error = True
                oldest_query.error = str(ex)
            finally:
                backend.close_user_connection(user)

            try:
                oldest_query.is_finished = True
                oldest_query.date_finished = timezone.now()
                oldest_query.rows_total = row_count
                oldest_query.save()

                if oldest_query.is_preview_for:
                    dataset = oldest_query.is_preview_for
                    dataset.preview_is_finished = True
                    dataset.preview_error = oldest_query.error
                    # Make sure all current users can see the preview table
                    reset_dataset_account_access(dataset)
                    dataset.save()
            except Exception as ex:
                print("Error: %s" % str(ex))

            if verbose:
                print("Finished query id %s." % oldest_query.pk)
            if run_once:
                keep_looping = False

    def periodic_check():
        """
        Every 5 seconds, do a check for new queries.  Just in case something
        needs processing, but didn't call trigger_processing() itself.
        """
        while True:
            sleep(5)
            if verbose:
                print("Triggering periodic processing.")
            trigger_query_queue_processing()

    q = Queue()
    filtered = Query.objects.filter(is_finished=False)
    if run_once:
        try:
            oldest_query = filtered.order_by('id')[:1].get()
        except Query.DoesNotExist:
            return
        q.put(oldest_query)
        worker(q)

    else:
        # Track the oldest query, so we only select ones newer that
        newest_pk = 0
        for i in range(thread_count):
            t = Process(target=worker, args=(q,))
            t.start()

        # Start with any queries already in the queue:
        for query in filtered:
            if query.pk > newest_pk:
                newest_pk = query.pk
            if verbose:
                print("Adding query ID %s to the queue." % query.pk)
            q.put(query)

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

        server.listen(5)
        while True:
            (clientsocket, address) = server.accept()
            # We don't actually have a protocol to speak...
            clientsocket.close()
            queries = Query.objects.filter(is_finished=False, pk__gt=newest_pk)
            for query in queries:
                if query.pk > newest_pk:
                    newest_pk = query.pk
                if verbose:
                    print("Adding query ID %s to the queue." % query.pk)
                q.put(query)
