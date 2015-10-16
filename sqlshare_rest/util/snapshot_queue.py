from sqlshare_rest.models import Dataset
from sqlshare_rest.dao.dataset import create_preview_for_dataset
from sqlshare_rest.util.db import get_backend
from time import sleep
from sqlshare_rest.util.queue_triggers import trigger_snapshot_processing
from sqlshare_rest.util.queue_triggers import SNAPSHOT_QUEUE_PORT_NUMBER
from django.conf import settings
import atexit
import time
import sys
import os

import socket
from threading import Thread

TERMINATE_TRIGGER_FILE = getattr(settings,
                                 "SQLSHARE_TERMINATE_QUERY_QUEUE_PATH",
                                 "/tmp/sqlshare_terminate_snapshot_queue")


def process_snapshot_queue(thread_count=0, run_once=True, verbose=False):
    # Make sure only one instance is running at a time:
    if trigger_snapshot_processing():
        return

    def start_snapshot(snapshot, background=True):
        """
        Get a dataset snapshot from the queue, and materialize its table.
        """
        snapshot.snapshot_started = True
        snapshot.save()
        snapshot_id = snapshot.pk

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

        backend = get_backend()
        oldest = Dataset.objects.get(pk=snapshot_id)

        if verbose:
            print("Processing snapshot: %s" % oldest.pk)
        user = oldest.owner
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

        if verbose:
            print("Finished snapshot %s." % oldest.pk)

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
        start_snapshot(oldest, background=False)
    else:
        # Start with any queries already in the queue:
        for dataset in filtered:
            if verbose:
                print("Adding dataset ID %s to the queue." % dataset.pk)

            start_snapshot(dataset)

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
            # Check to see if we should exit...
            if os.path.isfile(TERMINATE_TRIGGER_FILE):
                sys.exit(0)

            snapshots = Dataset.objects.filter(snapshot_source__isnull=False,
                                               snapshot_started=False)
            for snapshot in snapshots:
                if verbose:
                    print("Adding snapshot ID %s to the queue." % snapshot.pk)
                start_snapshot(snapshot)


def get_initial_filter_list():
    return Dataset.objects.filter(snapshot_source__isnull=False,
                                  snapshot_started=False)


def kill_snapshot_queue():
    # Create the file that triggers the termination
    f = open(TERMINATE_TRIGGER_FILE, "w")
    f.write("OK")
    f.close()

    # Trigger the check...
    trigger_snapshot_processing()

    # Just a quick pause before polling
    time.sleep(0.3)

    # Poll to see if the process is still running...
    for i in range(10):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(('localhost', SNAPSHOT_QUEUE_PORT_NUMBER))
            time.sleep(1)
        except socket.error as ex:
            os.remove(TERMINATE_TRIGGER_FILE)
            return True

    os.remove(TERMINATE_TRIGGER_FILE)
    return False
