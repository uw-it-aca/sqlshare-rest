from sqlshare_rest.util.dataset_emails import send_new_emails
from django.core.management.base import BaseCommand
from optparse import make_option
from time import sleep


class Command(BaseCommand):
    help = "This runs queries that users initiate in the front-end"

    def add_arguments(self, parser):
        parser.add_argument('--run-once',
                            dest='run_once',
                            default=False,
                            action="store_true",
                            help='Only process one item in the queue')

    def handle(self, *args, **options):
        keep_looping = True

        while keep_looping:
            send_new_emails()
            if options["run_once"]:
                keep_looping = False
            else:
                sleep(5)
