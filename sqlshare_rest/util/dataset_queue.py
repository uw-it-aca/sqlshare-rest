from sqlshare_rest.models import FileUpload
from sqlshare_rest.parser import Parser
from sqlshare_rest.util.db import get_backend
from sqlshare_rest.dao.dataset import create_dataset_from_query


def process_dataset_queue():
    filtered = FileUpload.objects.filter(dataset_created=False,
                                         is_finalized=True)

    try:
        oldest = filtered.order_by('id')[:1].get()
    except FileUpload.DoesNotExist:
        return

    user = oldest.owner
    backend = get_backend()
    try:
        p = Parser()
        p.delimiter(oldest.delimiter)
        p.has_header_row(oldest.has_column_header)

        file_path = oldest.user_file.path
        handle = open(file_path, "rt")
        handle.seek(0)
        p.parse(handle)

        table_name = backend.create_table_from_parser(oldest.dataset_name,
                                                      p,
                                                      user)

        dataset_sql = backend.get_view_sql_for_dataset(table_name, user)
        dataset = create_dataset_from_query(user.username,
                                            oldest.dataset_name,
                                            dataset_sql)

        oldest.dataset = dataset
        oldest.dataset_created = True
        oldest.save()
    except Exception as ex:
        oldest.has_error = True
        oldest.error = str(ex)
        oldest.save()
    finally:
        backend.close_user_connection(user)
