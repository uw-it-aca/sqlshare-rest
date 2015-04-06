from sqlshare_rest.util.query_queue import process_queue
from django.core.management.base import BaseCommand
from optparse import make_option


class Command(BaseCommand):
    help = "This runs queries that users initiate in the front-end"

    option_list = BaseCommand.option_list + (
        make_option('--run-once',
                    dest='run_once',
                    default=False,
                    action="store_true",
                    help='This will only process one item in the queue'),
                    )

    def handle(self, *args, **options):
        if not options["run_once"]:
            raise Exception("Only handles run_once for now")

        process_queue()
