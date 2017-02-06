from django.core.management.base import BaseCommand
from sqlshare_rest.util.query_queue import kill_query_queue
from sqlshare_rest.util.dataset_queue import kill_dataset_queue
from sqlshare_rest.util.snapshot_queue import kill_snapshot_queue


class Command(BaseCommand):
    help = "This terminates the queue queue daemon"

    def handle(self, *args, **kwargs):
        kill_query_queue()
        kill_dataset_queue()
        kill_snapshot_queue()
