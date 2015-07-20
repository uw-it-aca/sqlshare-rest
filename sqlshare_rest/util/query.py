# This module exists to prevent circular imports.
from sqlshare_rest.util.db import get_backend
import json


def get_sample_data_for_query(query, username):
    backend = get_backend()
    user_obj = backend.get_user(username)

    if query.is_finished and not query.error:
        data = json.loads(query.preview_content)

        return data["data"], data["columns"]
        try:
            cursor = get_query_sample_data(user_obj, query.id)
            data = []
            row = cursor.fetchone()
            while row:
                # This is to make sure the data is json serializable.  Some
                # backends return 'row' objects, such as <type 'pyodbc.Row'>
                data.append(list(row))
                row = cursor.fetchone()
            columns = frontend_description_from_cursor(cursor)
            cursor.close()
        except Exception as ex:
            # This should only come up in backends that have data permissions
            # but don't support public acces.
            return (None, None)
        finally:
            backend.close_user_connection(user_obj)
        return (data, columns)
    return (None, None)


def get_query_sample_data(user, id):
    backend = get_backend()
    return backend.get_query_sample(user, id)


def frontend_description_from_cursor(cursor):
    columns = []
    for col in cursor.description:
        columns.append({
            "name":  col[0],
        })

    return columns
