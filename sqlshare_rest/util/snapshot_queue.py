from sqlshare_rest.models import Dataset
from sqlshare_rest.dao.dataset import create_preview_for_dataset
from sqlshare_rest.util.db import get_backend
from time import sleep
from sqlshare_rest.util.queue_triggers import trigger_snapshot_processing
from sqlshare_rest.util.queue_triggers import SNAPSHOT_QUEUE_PORT_NUMBER
import atexit

import socket
from threading import Thread

import six

if six.PY2:
    from Queue import Queue
elif six.PY3:
    from queue import Queue


def process_snapshot_queue(thread_count=0, run_once=True, verbose=False):
    q = Queue()

    def worker():
        """
        Get a dataset snapshot from the queue, and materialize its table.
        """
        backend = get_backend()
        keep_looping = True
        while keep_looping:
            oldest = q.get()
            if verbose:
                print("Processing snapshot: %s" % oldest.pk)
            user = oldest.owner
            backend = get_backend()
            try:
                backend.load_snapshot_table(oldest, user)
                create_preview_for_dataset(oldest)
                oldest.snapshot_finished = True
                oldest.save()
                # Do some work here
            except Exception as ex:
                if verbose:
                    print("Error on %s: %s" % (oldest.pk, str(ex)))
                oldest.snapshot_finished = True
                oldest.save()
            finally:
                backend.close_user_connection(user)

            q.task_done()
            if verbose:
                print("Finished snapshot %s." % oldest.pk)
            if run_once:
                keep_looping = False

    def periodic_check():
        """
        Every 5 seconds, do a check for new snapshots.  Just in case something
        needs processing, but didn't call trigger_snapshot_processing() itself.
        """
        while True:
            sleep(5)
            if verbose:
                print("Triggering periodic processing.")
            trigger_snapshot_processing()

    filtered = get_initial_filter_list()

    if run_once:
        try:
            oldest = filtered.order_by('id')[:1].get()
        except Dataset.DoesNotExist:
            return
        q.put(oldest)
        worker()
    else:
        # Track the oldest query, so we only select ones newer that
        newest_pk = 0
        for i in range(thread_count):
            t = Thread(target=worker)
            t.setDaemon(True)
            t.start()

        # Start with any queries already in the queue:
        for dataset in filtered:
            if dataset.pk > newest_pk:
                newest_pk = dataset.pk
            if verbose:
                print("Adding dataset ID %s to the queue." % upload.pk)
            q.put(dataset)

        # Just in case things get off the rails - maybe a connection to the
        # server gets blocked? - periodically trigger a check for new queries
        kicker = Thread(target=periodic_check)
        kicker.setDaemon(True)
        kicker.start()

        # Start the socket server for getting notifications of new queries
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Make it so we can run the server right away after killing it
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('localhost', SNAPSHOT_QUEUE_PORT_NUMBER))

        # Make sure we close our socket when we're killed.
        def close_socket():
            server.close()

        atexit.register(close_socket)

        server.listen(5)
        while True:
            (clientsocket, address) = server.accept()
            # We don't actually have a protocol to speak...
            clientsocket.close()

            snapshots = Dataset.objects.filter(snapshot_source__isnull=False,
                                               snapshot_finished=False,
                                               pk__gt=newest_pk)
            for snapshot in snapshots:
                if snapshot.pk > newest_pk:
                    newest_pk = snapshot.pk
                if verbose:
                    print("Adding snapshot ID %s to the queue." % snapshot.pk)
                q.put(snapshot)

    q.join()


def get_initial_filter_list():
    return Dataset.objects.filter(snapshot_source__isnull=False,
                                  snapshot_finished=False)
