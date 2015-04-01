from sqlshare_rest.util.db import get_backend
from sqlshare_rest.models import Dataset, User, SharingEmail
from sqlshare_rest.exceptions import InvalidAccountException


def get_datasets_owned_by_user(user):
    # Django auth user vs sqlshare user
    backend = get_backend()
    user_obj = backend.get_user(user.username)
    return Dataset.objects.filter(owner=user_obj)


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


def set_dataset_accounts(dataset, accounts, save_dataset=True):
    backend = get_backend()
    # Get a unique list of values...
    # The set is used for removing access
    account_set = set(accounts)
    accounts = list(set(accounts))
    user_models = User.objects.filter(username__in=accounts)

    if len(user_models) != len(accounts):
        raise InvalidAccountException()

    # XXX - put this in a transaction?
    current_users = dataset.shared_with.all()
    for user in current_users:
        if user.username not in account_set:
            backend.remove_access_to_dataset(dataset.name,
                                             owner=dataset.owner,
                                             reader=user)

    for user in user_models:
        backend.add_read_access_to_dataset(dataset.name,
                                           owner=dataset.owner,
                                           reader=user)

    dataset.shared_with = user_models
    if save_dataset:
        dataset.save()


def set_dataset_emails(dataset, emails, save_dataset=True):
    # Get a unique list...
    emails = list(set(emails))
    existing_models = list(SharingEmail.objects.filter(email__in=emails))

    # Fill in the gaps
    if len(existing_models) != len(emails):
        existing_set = set(map(lambda x: x.email, existing_models))

        for email in emails:
            if email not in existing_set:
                new = SharingEmail.objects.create(email=email)
                existing_models.append(new)

    dataset.email_shares = existing_models
    if save_dataset:
        dataset.save()
