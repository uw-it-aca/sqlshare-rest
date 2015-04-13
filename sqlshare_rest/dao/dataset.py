from sqlshare_rest.util.db import get_backend
from sqlshare_rest.models import Dataset, User, SharingEmail, DatasetTag, Tag
from sqlshare_rest.models import Query
from sqlshare_rest.exceptions import InvalidAccountException


def get_datasets_owned_by_user(user):
    # Django auth user vs sqlshare user
    backend = get_backend()
    user_obj = backend.get_user(user.username)
    return Dataset.objects.filter(owner=user_obj)


def get_datasets_shared_with_user(user):
    # Django auth user vs sqlshare user
    backend = get_backend()
    user_obj = backend.get_user(user.username)
    return Dataset.objects.filter(shared_with__in=[user_obj])


def get_public_datasets():
    return Dataset.objects.filter(is_public=True)


def get_all_datasets_for_user(user):
    datasets = list(get_datasets_owned_by_user(user))
    datasets.extend(list(get_datasets_shared_with_user(user)))
    datasets.extend(list(get_public_datasets()))

    return list(set(datasets))


def get_dataset_by_owner_and_name(owner, name):
    user_obj = User.objects.get(username=owner)
    dataset = Dataset.objects.get(owner=user_obj, name=name)
    return dataset


def create_dataset_from_query(username, dataset_name, sql):
    backend = get_backend()
    user = backend.get_user(username)
    try:
        (model, created) = Dataset.objects.get_or_create(name=dataset_name,
                                                         owner=user)

        if not created:
            # Clear out the existing dataset, so we can create
            # the new view properly
            backend.delete_dataset(dataset_name, user)

        backend.create_view(dataset_name, sql, user)

        model.sql = sql
        model.save()

        # Remove all existing sample data queries
        previous = Query.objects.filter(is_preview_for=model)
        for query in previous:
            backend.delete_query(query.pk)
            query.delete()

        preview_sql = backend.get_preview_sql_for_query(sql)
        query_obj = Query.objects.create(sql=preview_sql,
                                         owner=user,
                                         is_preview_for=model)

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

    try:
        query = Query.objects.get(is_preview_for=dataset)
        if not query.is_finished:
            # Don't try setting permissions on a query that might not
            # exist yet.
            query = None
    except Query.DoesNotExist:
        query = None
    # XXX - put this in a transaction?
    current_users = dataset.shared_with.all()
    for user in current_users:
        if user.username not in account_set:
            backend.remove_access_to_dataset(dataset.name,
                                             owner=dataset.owner,
                                             reader=user)
        if query:
            backend.remove_read_access_to_query(query.pk, user)

    for user in user_models:
        backend.add_read_access_to_dataset(dataset.name,
                                           owner=dataset.owner,
                                           reader=user)

        if query:
            backend.add_read_access_to_query(query.pk, user)

    dataset.shared_with = user_models
    if save_dataset:
        dataset.save()


def add_public_access(dataset):
    try:
        get_backend().add_public_access(dataset.name, dataset.owner.username)
    except AttributeError:
        pass
    dataset.is_public = True
    dataset.save()


def remove_public_access(dataset):
    try:
        username = dataset.owner.username
        get_backend().remove_public_access(dataset.name, username)
    except AttributeError:
        pass
    dataset.is_public = False
    dataset.save()


def reset_dataset_account_access(dataset):
    backend = get_backend()
    try:
        query = Query.objects.get(is_preview_for=dataset)
        if not query.is_finished:
            # Don't try setting permissions on a query that might not
            # exist yet.
            query = None
    except Query.DoesNotExist:
        query = None
    # XXX - put this in a transaction?
    current_users = dataset.shared_with.all()
    for user in current_users:
        backend.add_read_access_to_dataset(dataset.name,
                                           owner=dataset.owner,
                                           reader=user)

        if query:
            backend.add_read_access_to_query(query.pk, user)


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


def update_tags(dataset, change_list, user):
    # Update the owner's tags last, in case they remove a tag
    # from a user, and then add it to their own list
    owner_tags = None
    for item in change_list:
        username = item["name"]
        tags = item["tags"]

        if username == dataset.owner.username:
            owner_tags = item
        else:
            can_add = False
            if username == user.username:
                can_add = True
            _update_tag_list(dataset, item, can_add=can_add)

    if owner_tags:
        _update_tag_list(dataset, owner_tags, can_add=True)


def _update_tag_list(dataset, tag_item, can_add):
    username = tag_item["name"]
    tags = tag_item["tags"]

    try:
        user = User.objects.get(username=username)
    except User.DoesNotExist:
        return

    # Get all their existing tags, so we know what to remove/update the
    # popularity of
    existing_list = DatasetTag.objects.filter(dataset=dataset, user=user)
    existing_lookup = {}
    for dataset_tag in existing_list:
        existing_lookup[dataset_tag.tag.tag] = dataset_tag

    for tag_label in tags:
        if tag_label in existing_lookup:
            del(existing_lookup[tag_label])
        else:
            if can_add:
                _create_tag(dataset, user, tag_label)

    for tag in existing_lookup:
        dataset_tag = existing_lookup[tag]
        dataset_tag.delete()

        _update_tag_popularity(tag)


def _create_tag(dataset, user, tag_label):
    (tag_obj, created) = Tag.objects.get_or_create(tag=tag_label)

    dataset_tag = DatasetTag.objects.get_or_create(tag=tag_obj,
                                                   user=user,
                                                   dataset=dataset)
    _update_tag_popularity(tag_label)


def _update_tag_popularity(tag_label):
    tag_obj = Tag.objects.get(tag=tag_label)
    count = DatasetTag.objects.filter(tag=tag_obj).count()
    tag_obj.popularity = count
    tag_obj.save()
