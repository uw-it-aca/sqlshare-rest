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
                    )

    def handle(self, *args, **options):
        if not options["run_once"]:
            raise Exception("Only handles --run-once for now")

        process_dataset_queue()
