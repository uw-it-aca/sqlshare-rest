from sqlshare_rest.util.snapshot_queue import process_snapshot_queue
from django.core.management.base import BaseCommand
from optparse import make_option


class Command(BaseCommand):
    help = "This processes snapshots of datasets."

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
                    )

    def handle(self, *args, **options):
        verbose = options["verbose"]
        if options["run_once"]:
            process_snapshot_queue(verbose=verbose)
        else:
            process_snapshot_queue(run_once=False,
                                   thread_count=10,
                                   verbose=verbose)
