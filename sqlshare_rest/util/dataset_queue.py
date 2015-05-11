from sqlshare_rest.models import FileUpload
from sqlshare_rest.parser import Parser
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.dao.dataset import create_dataset_from_query
from time import sleep
from sqlshare_rest.util.queue_triggers import trigger_upload_queue_processing
from sqlshare_rest.util.queue_triggers import UPLOAD_QUEUE_PORT_NUMBER
import atexit

import socket
from threading import Thread

import six

if six.PY2:
    from Queue import Queue
elif six.PY3:
    from queue import Queue


def process_dataset_queue(thread_count=0, run_once=True, verbose=False):
    q = Queue()

    def worker():
        """
        Get a file upload object from the queue, and turn it into a dataset.
        """
        backend = get_backend()
        keep_looping = True
        while keep_looping:
            oldest = q.get()
            if verbose:
                print("Processing file upload: %s" % oldest.pk)
            user = oldest.owner
            backend = get_backend()
            try:
                p = Parser()
                p.delimiter(oldest.delimiter)
                p.has_header_row(oldest.has_column_header)

                file_path = oldest.user_file.path
                handle = open(file_path, "rt")
                handle.seek(0)
                p.parse(handle)

                name = oldest.dataset_name
                table_name = backend.create_table_from_parser(name,
                                                              p,
                                                              user)

                dataset_sql = backend.get_view_sql_for_dataset(table_name,
                                                               user)
                dataset = create_dataset_from_query(user.username,
                                                    oldest.dataset_name,
                                                    dataset_sql)

                oldest.dataset = dataset
                oldest.dataset_created = True
                oldest.save()
            except Exception as ex:
                if verbose:
                    print("Error on %s: %s" % (oldest.pk, str(ex)))
                oldest.has_error = True
                oldest.error = str(ex)
                oldest.save()
            finally:
                backend.close_user_connection(user)

            q.task_done()
            if verbose:
                print("Finished file upload %s." % oldest.pk)
            if run_once:
                keep_looping = False

    def periodic_check():
        """
        Every 5 seconds, do a check for new files.  Just in case something
        needs processing, but didn't call trigger_processing() itself.
        """
        while True:
            sleep(5)
            if verbose:
                print("Triggering periodic processing.")
            trigger_upload_queue_processing()

    filtered = FileUpload.objects.filter(dataset_created=False,
                                         is_finalized=True)

    if run_once:
        try:
            oldest = filtered.order_by('id')[:1].get()
        except FileUpload.DoesNotExist:
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
        for upload in filtered:
            if upload.pk > newest_pk:
                newest_pk = upload.pk
            if verbose:
                print("Adding file upload ID %s to the queue." % upload.pk)
            q.put(upload)

        # Just in case things get off the rails - maybe a connection to the
        # server gets blocked? - periodically trigger a check for new queries
        kicker = Thread(target=periodic_check)
        kicker.setDaemon(True)
        kicker.start()

        # Start the socket server for getting notifications of new queries
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Make it so we can run the server right away after killing it
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('localhost', UPLOAD_QUEUE_PORT_NUMBER))

        # Make sure we close our socket when we're killed.
        def close_socket():
            server.close()

        atexit.register(close_socket)

        server.listen(5)
        while True:
            (clientsocket, address) = server.accept()
            # We don't actually have a protocol to speak...
            clientsocket.close()

            uploads = FileUpload.objects.filter(dataset_created=False,
                                                is_finalized=True,
                                                pk__gt=newest_pk)
            for upload in uploads:
                if upload.pk > newest_pk:
                    newest_pk = upload.pk
                if verbose:
                    print("Adding upload ID %s to the queue." % upload.pk)
                q.put(upload)

    q.join()
