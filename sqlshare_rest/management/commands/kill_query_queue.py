from django.core.management.base import BaseCommand
from sqlshare_rest.util.query_queue import kill_query_queue


class Command(BaseCommand):
    help = "This terminates the queue queue daemon"

    def handle(self, *args, **kwargs):
        kill_query_queue()
