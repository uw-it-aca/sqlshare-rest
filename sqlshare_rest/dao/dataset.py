from sqlshare_rest.util.db import get_backend
from sqlshare_rest.models import Dataset, User


def get_dataset_by_owner_and_name(owner, name):
    user_obj = User.objects.get(username=owner)
    dataset = Dataset.objects.get(owner=user_obj, name=name)
    return dataset


def create_dataset_from_query(username, dataset_name, sql):
    backend = get_backend()
    user = backend.get_user(username)
    try:
        backend.create_view(dataset_name, sql, user)

        (model, created) = Dataset.objects.get_or_create(name=dataset_name,
                                                         owner=user)
        model.sql = sql
        model.save()
        return model
    except Exception:
        raise
    finally:
        backend.close_user_connection(user)
