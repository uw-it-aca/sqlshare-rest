from sqlshare_rest.util.dataset_queue import process_dataset_queue
from django.core.management.base import BaseCommand
from optparse import make_option


class Command(BaseCommand):
    help = "This loads data into datasets from uploaded files."

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
            process_dataset_queue(verbose=verbose)
        else:
            process_dataset_queue(run_once=False,
                                  thread_count=10,
                                  verbose=verbose)
