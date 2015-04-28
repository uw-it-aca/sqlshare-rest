from sqlshare_rest.util.db import get_backend
from sqlshare_rest.models import Query
from sqlshare_rest.dao.dataset import reset_dataset_account_access
from django.utils import timezone
from time import sleep
from sqlshare_rest.util.queue_triggers import trigger_query_queue_processing
import signal

import socket
from threading import Thread

import six

if six.PY2:
    from Queue import Queue
elif six.PY3:
    from queue import Queue

PORT_NUMBER = 1999


def process_queue(thread_count=0, run_once=True, verbose=False):
    q = Queue()

    def worker():
        """
        Get a query from the queue, and process it...
        """
        backend = get_backend()
        keep_looping = True
        while keep_looping:
            oldest_query = q.get()
            if verbose:
                print ("Processing query id %s." % oldest_query.pk)
            user = oldest_query.owner
            try:
                cursor = backend.run_query(oldest_query.sql,
                                           user,
                                           return_cursor=True)

                name = "query_%s" % oldest_query.pk
                try:
                    backend.create_table_from_query_result(name, cursor)
                    backend.add_read_access_to_query(oldest_query.pk, user)
                except:
                    raise
            except Exception as ex:
                oldest_query.has_error = True
                oldest_query.error = str(ex)
            finally:
                backend.close_user_connection(user)

            try:
                oldest_query.is_finished = True
                oldest_query.date_finished = timezone.now()
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

            q.task_done()
            if verbose:
                print ("Finished query id %s." % oldest_query.pk)
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
                print ("Triggering periodic processing.")
            trigger_query_queue_processing()

    filtered = Query.objects.filter(is_finished=False)
    if run_once:
        try:
            oldest_query = filtered.order_by('id')[:1].get()
        except Query.DoesNotExist:
            return
        q.put(oldest_query)
        worker()

    else:
        # Track the oldest query, so we only select ones newer that
        newest_pk = 0
        for i in range(thread_count):
            t = Thread(target=worker)
            t.setDaemon(True)
            t.start()

        # Start with any queries already in the queue:
        for query in filtered:
            if query.pk > newest_pk:
                newest_pk = query.pk
            if verbose:
                print ("Adding query ID %s to the queue." % query.pk)
            q.put(query)

        # Just in case things get off the rails - maybe a connection to the
        # server gets blocked? - periodically trigger a check for new queries
        kicker = Thread(target=periodic_check)
        kicker.setDaemon(True)
        kicker.start()

        # Start the socket server for getting notifications of new queries
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(('localhost', PORT_NUMBER))

        # Make sure we close our socket when we're killed.
        def sig_handler(signal, frame):
            server.close()

        signal.signal(signal.SIGINT, sig_handler)


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
                    print ("Adding query ID %s to the queue." % query.pk)
                q.put(query)

    q.join()
