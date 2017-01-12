from sqlshare_rest.models import FileUpload
from sqlshare_rest.parser import Parser, open_encoded
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.dao.dataset import create_dataset_from_query
from time import sleep
from sqlshare_rest.util.queue_triggers import trigger_upload_queue_processing
from sqlshare_rest.util.queue_triggers import UPLOAD_QUEUE_PORT_NUMBER
from sqlshare_rest.logger import getLogger
from django.template.loader import render_to_string
from django.core.mail import EmailMultiAlternatives
from django.db.utils import DatabaseError
from django.conf import settings
from django import db
import atexit
import time
import sys
import os
import re

import socket
from threading import Thread

import six

if six.PY2:
    from urllib import quote
if six.PY3:
    from urllib.parse import quote

TERMINATE_TRIGGER_FILE = getattr(settings,
                                 "SQLSHARE_TERMINATE_UPLOAD_QUEUE_PATH",
                                 "/tmp/sqlshare_terminate_upload_queue")

logger = getLogger(__name__)


def process_dataset_queue(thread_count=0, run_once=True, verbose=False):
    # Make sure only one instance is running at a time:
    if trigger_upload_queue_processing():
        return

    def email_owner_success(dataset):
        to = dataset.owner.get_email()
        values = {}

        url_format = getattr(settings,
                             "SQLSHARE_DETAIL_URL_FORMAT",
                             "https://sqlshare.uw.edu/detail/%s/%s")

        url = url_format % (quote(dataset.owner.username), quote(dataset.name))

        values['url'] = url
        values['name'] = dataset.name

        text_version = render_to_string('uploaded_email/text.html', values)
        html_version = render_to_string('uploaded_email/html.html', values)
        subject = render_to_string('uploaded_email/subject.html', values)
        subject = re.sub(r'[\s]*$', '', subject)
        from_email = "sqlshare-noreply@uw.edu"
        msg = EmailMultiAlternatives(subject, text_version, from_email, [to])
        msg.attach_alternative(html_version, "text/html")
        try:
            msg.send()
        except Exception as ex:
            logger.error("Unable to send email to %s.  Error: %s" % (to,
                                                                     str(ex)))

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

        success = False
        try:
            process_upload(upload_id)
            success = True
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
            import traceback
            tb = traceback.format_exc()
            logger.error("Error on %s: %s (%s)" % (upload_id, str(ex), tb))

        if success:
            try:
                # Get a new upload object, to get the dataset
                saved = FileUpload.objects.get(pk=upload.pk)
                email_owner_success(saved.dataset)
            except Exception as ex:
                print ex
                logger.error("Error emailing on upload success: %s" % ex)

        if background:
            sys.exit(0)

    def process_upload(upload_id):
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
            handle = open_encoded(file_path, "U")
            handle.seek(0)
            p.parse(handle)

            name = upload.dataset_name
            table = backend.create_table_from_parser(name,
                                                     p,
                                                     upload,
                                                     user)

            dataset_sql = backend.get_view_sql_for_dataset_by_parser(table,
                                                                     p,
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
            import traceback
            tb = traceback.format_exc()
            msg = "Error on %s: %s - %s" % (upload.pk, str(ex), tb)
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
            except Exception as ex:
                # This was originally DatabaseError - but then there were also
                # pyodbc.Error exceptions... and pyodbc isn't a hard
                # requirement.
                ex_str = str(ex)
                # If there's just, say, a network glitch, carry on.
                # Or, say, a server restart
                # If it's anything else, re-raise the error.
                is_reset_error = False
                if ex_str.find("Read from the server failed") >= 0:
                    is_reset_error = True
                if ex_str.find("Write to the server failed") >= 0:
                    is_reset_error = True
                if ex_str.find("Communication link failure") >= 0:
                    is_reset_error = True

                adaptive = "Adaptive Server is unavailable or does not exist"
                if ex_str.find(adaptive) >= 0:
                    is_reset_error = True

                if is_reset_error:
                    try:
                        db.close_old_connections()
                    except Exception as ex:
                        ex_str = str(ex)
                        is_expected = False
                        rollback_err = "Could not perform COMMIT or ROLLBACK"
                        if ex_str.find(rollback_err) >= 0:
                            # db.close_connection tries to end transactions
                            # pyodbc was absolutely unable to recover from that
                            # because it wasn't reconnecting to be able to do
                            # the rollback...
                            from django.db import connections
                            for conn in connections:
                                connections[conn].close()

                        else:
                            logger.error("Error in dataset queue: %s" % ex_str)
                            raise
                else:
                    logger.error("Error in dataset queue: %s" % ex_str)
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
