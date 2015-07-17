from sqlshare_rest.models import FileUpload
from sqlshare_rest.parser import Parser
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.dao.dataset import create_dataset_from_query
from time import sleep
from sqlshare_rest.util.queue_triggers import trigger_upload_queue_processing
from sqlshare_rest.util.queue_triggers import UPLOAD_QUEUE_PORT_NUMBER
from sqlshare_rest.logger import getLogger
from django.db.utils import DatabaseError
from django.conf import settings
import atexit
import time
import sys
import os

import socket
from threading import Thread

TERMINATE_TRIGGER_FILE = getattr(settings,
                                 "SQLSHARE_TERMINATE_UPLOAD_QUEUE_PATH",
                                 "/tmp/sqlshare_terminate_upload_queue")


def process_dataset_queue(thread_count=0, run_once=True, verbose=False):
    def start_upload(upload, background=True):
        upload.is_started = True
        upload.save()
        upload_id = upload.pk
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
            process_upload(upload_id)
        except Exception as ex:
            try:
                upload = FileUpload.objects.get(pk=upload_id)
                upload.has_error = True
                upload.error = str(ex)
                upload.is_finished = True
                upload.save()
            except:
                # That try is just trying to get info out to the user, it's
                # relatively ok if that fails
                pass
            logger = getLogger(__name__)
            logger.error("Error on %s: %s" % (upload_id, str(ex)))

        if background:
            sys.exit(0)

    def process_upload(upload_id):
        logger = getLogger(__name__)
        upload = FileUpload.objects.get(pk=upload_id)
        msg = "Processing file upload: %s" % upload.pk
        logger.info(msg)
        user = upload.owner
        backend = get_backend()
        try:
            p = Parser()
            p.delimiter(upload.delimiter)
            p.has_header_row(upload.has_column_header)

            file_path = upload.user_file.path
            handle = open(file_path, "rt")
            handle.seek(0)
            p.parse(handle)

            name = upload.dataset_name
            table_name = backend.create_table_from_parser(name,
                                                          p,
                                                          upload,
                                                          user)

            dataset_sql = backend.get_view_sql_for_dataset(table_name,
                                                           user)
            dataset = create_dataset_from_query(user.username,
                                                upload.dataset_name,
                                                dataset_sql)

            dataset.description = upload.dataset_description
            dataset.is_public = upload.dataset_is_public
            dataset.save()
            upload.dataset = dataset
            upload.dataset_created = True
            upload.save()
        except Exception as ex:
            msg = "Error on %s: %s" % (upload.pk, str(ex))
            logger.error(msg)
            upload.has_error = True
            upload.error = str(ex)
            upload.save()
        finally:
            backend.close_user_connection(user)

        msg = "Finished file upload %s." % upload.pk
        logger.info(msg)

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

    filtered = FileUpload.objects.filter(is_started=False, is_finalized=True)

    if run_once:
        try:
            oldest = filtered.order_by('id')[:1].get()
        except FileUpload.DoesNotExist:
            return

        start_upload(oldest, background=False)
    else:
        # Start with any queries already in the queue:
        for upload in filtered:
            start_upload(upload)

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
            # Check to see if we should exit...
            if os.path.isfile(TERMINATE_TRIGGER_FILE):
                sys.exit(0)

            # We don't actually have a protocol to speak...
            clientsocket.close()

            try:
                uploads = FileUpload.objects.filter(is_started=False,
                                                    is_finalized=True)

                for upload in uploads:
                    start_upload(upload)
            except DatabaseError as ex:
                ex_str = str(ex)
                # If there's just, say, a network glitch, carry on.
                # If it's anything else, re-raise the error.
                if str_ex.find("Read from the server failed") < 0:
                    raise


def kill_dataset_queue():
    # Create the file that triggers the termination
    f = open(TERMINATE_TRIGGER_FILE, "w")
    f.write("OK")
    f.close()

    # Trigger the check...
    trigger_upload_queue_processing()

    # Just a quick pause before polling
    time.sleep(0.3)

    # Poll to see if the process is still running...
    for i in range(10):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(('localhost', UPLOAD_QUEUE_PORT_NUMBER))
            time.sleep(1)
        except socket.error as ex:
            os.remove(TERMINATE_TRIGGER_FILE)
            return True

    os.remove(TERMINATE_TRIGGER_FILE)
    return False
