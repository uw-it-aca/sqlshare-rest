from sqlshare_rest.util.query_queue import process_queue
from django.core.management.base import BaseCommand
from optparse import make_option
import os


class Command(BaseCommand):
    help = "This runs queries that users initiate in the front-end"

    option_list = BaseCommand.option_list + (
        make_option('--run-once',
                    dest='run_once',
                    default=False,
                    action="store_true",
                    help='This will only process one item in the queue'),
        make_option('--verbose',
                    dest='verbose',
                    default=False,
                    action="store_true",
                    help='Prints status info to standard out'),
        make_option('--daemonize',
                    dest='daemon',
                    default=False,
                    action="store_true",
                    help='Run in the background'),
                    )


    def handle(self, *args, **options):
        verbose = options["verbose"]

        if options["daemon"]:
            pid = os.fork()
            if pid == 0:
                os.setsid()

                pid = os.fork()
                if pid != 0:
                    os._exit(0)

            else:
                os._exit(0)

        if options["run_once"]:
            process_queue(verbose=verbose)
        else:
            process_queue(run_once=False, thread_count=5, verbose=verbose)
