from sqlshare_rest.util.db import get_backend
from sqlshare_rest.models import Dataset, User, SharingEmail, DatasetTag, Tag
from sqlshare_rest.models import DatasetSharingEmail
from sqlshare_rest.models import Query, RecentDatasetView
from sqlshare_rest.exceptions import InvalidAccountException
from django.db.models import Q


def get_datasets_owned_by_user(user, request, page_list=True):
    # Django auth user vs sqlshare user
    backend = get_backend()
    user_obj = backend.get_user(user.username)
    base = Dataset.objects.filter(owner=user_obj)
    return _filter_list_from_request(base, request, page_list)


def get_datasets_shared_with_user(user, request, page_list=True):
    # Django auth user vs sqlshare user
    backend = get_backend()
    user_obj = backend.get_user(user.username)
    base = Dataset.objects.filter(shared_with__in=[user_obj])
    return _filter_list_from_request(base, request, page_list)


def get_public_datasets(request, page_list=True):
    base = Dataset.objects.filter(is_public=True)
    return _filter_list_from_request(base, request, page_list)


def get_paged_dataset_log_message(list_type, request):
    search = request.GET.get("q", "")
    order_by = request.GET.get("order_by", "updated")
    page_num = request.GET.get("page", "1")
    page_size = request.GET.get("page_size", "50")

    return ("GET %s dataset list; search '%s'; order_by: '%s'; "
            "page_num: %s, page_size: %s" % (list_type,
                                             search,
                                             order_by,
                                             page_num,
                                             page_size))


def get_recent_datasets_viewed_by_user(user, request, page_list=True):
    base = RecentDatasetView.objects.filter(user=user).order_by("-timestamp",
                                                                "-pk")

    # This needs to be different than _filter_list_from_request, because
    # of the foreign key to dataset...
    if "q" in request.GET:
        q = request.GET["q"]
        base = base .filter(Q(dataset__name__icontains=q) |
                            Q(dataset__description__icontains=q))

    paged = _page_dataset_list(base, request)

    return map(lambda x: x.dataset, paged)


def _get_all_dataset_querysets(user, request):
    return (get_datasets_owned_by_user(user, request, page_list=False),
            get_datasets_shared_with_user(user, request, page_list=False),
            get_public_datasets(request, page_list=False))


def _dataset_unique_list(mine, shared, public):
    return mine | shared | public


def get_all_datasets_for_user(user, request):
    mine, shared, public = _get_all_dataset_querysets(user, request)
    unique = _dataset_unique_list(mine, shared, public)

    return _page_dataset_list(unique, request)


def get_all_datasets_tagged_for_user(user, request, tag_label):
    try:
        tags = Tag.objects.filter(tag__iexact=tag_label)
    except Tag.DoesNotExist:
        return []
    datasets = get_all_datasets_for_user(user, request)

    dataset_tags = DatasetTag.objects.filter(dataset__in=datasets,
                                             tag__in=tags)

    values = list(map(lambda x: x.dataset, dataset_tags))

    return values


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

        row_count = backend.create_view(dataset_name, sql, user)
        model.rows_total = row_count

        model.preview_is_finished = False
        model.preview_error = None
        model.sql = sql
        model.save()

        create_preview_for_dataset(model)

        return model
    except Exception:
        raise
    finally:
        backend.close_user_connection(user)


def create_dataset_from_snapshot(user, dataset_name, source):
    backend = get_backend()
    try:
        (model, created) = Dataset.objects.get_or_create(name=dataset_name,
                                                         owner=user)
        if not created:
            # Clear out the existing dataset, so we can create
            # the new view properly
            backend.delete_dataset(dataset_name, user)

        backend.create_snapshot_dataset(source, model, user)

        model.preview_is_finished = False
        model.snapshot_started = False
        model.preview_error = None
        model.save()

        return model
    except Exception:
        raise
    finally:
        backend.close_user_connection(user)


def create_preview_for_dataset(dataset):
    # Remove all existing sample data queries
    previous = Query.objects.filter(is_preview_for=dataset)

    backend = get_backend()
    for query in previous:
        delete_id = query.pk
        query.delete()
        try:
            backend.delete_query(delete_id)
        except Exception as ex:
            pass

    dataset_name = dataset.name
    user = dataset.owner
    preview_sql = backend.get_preview_sql_for_dataset(dataset_name, user)
    query_obj = Query.objects.create(sql=preview_sql,
                                     owner=user,
                                     is_preview_for=dataset)


def set_dataset_accounts(dataset, accounts, save_dataset=True):
    backend = get_backend()
    # Get a unique list of values...
    # The set is used for removing access
    account_set = set(accounts)
    accounts = list(set(accounts))
    user_models = User.objects.filter(username__in=accounts)

    if len(user_models) != len(accounts):
        raise InvalidAccountException()

    query = get_preview_query(dataset)

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


def get_preview_query(dataset):
    try:
        query = Query.objects.get(is_preview_for=dataset)
        if not query.is_finished:
            # Don't try setting permissions on a query that might not
            # exist yet.
            query = None
    except Query.DoesNotExist:
        query = None

    return query


def add_account_to_dataset(dataset, account):
    if account == dataset.owner.username:
        return

    backend = get_backend()

    user = User.objects.get(username=account)
    query = get_preview_query(dataset)

    backend.add_read_access_to_dataset(dataset.name,
                                       owner=dataset.owner,
                                       reader=user)

    dataset.shared_with.add(user)


def add_public_access(dataset):
    try:
        get_backend().add_public_access(dataset, dataset.owner)
    except AttributeError as ex:
        pass
    dataset.is_public = True
    dataset.save()


def remove_public_access(dataset):
    try:
        get_backend().remove_public_access(dataset.name, dataset.owner)
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

    if dataset.is_public:
        add_public_access(dataset)
    else:
        remove_public_access(dataset)


def set_dataset_emails(dataset, emails, save_dataset=True):
    # Get a unique list...
    emails = list(set(emails))
    existing_email_models = list(SharingEmail.objects.filter(email__in=emails))

    remove_list = []
    existing_shares = DatasetSharingEmail.objects.filter(dataset=dataset)
    existing_shares_lookup = {}
    for share in existing_shares:
        existing_shares_lookup[share.email.email] = share

    # Make sure there's an Email object for each email:
    # Add any new DatasetSharingEmail objects needed
    existing_email_lookup = {}
    for obj in existing_email_models:
        existing_email_lookup[obj.email] = obj

    for email in emails:
        if email not in existing_email_lookup:
            new = SharingEmail.objects.create(email=email)
            existing_email_lookup[email] = new

        if email not in existing_shares_lookup:
            email_obj = existing_email_lookup[email]
            new_share = DatasetSharingEmail.objects.create(dataset=dataset,
                                                           email=email_obj)

    # Delete any DatasetSharingEmail objects that aren't currently valid
    # Keep the SharingEmail objects though
    for share_email in existing_shares_lookup:
        if share_email not in emails:
            existing_shares_lookup[share_email].delete()

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


def _filter_list_from_request(query_set, request, page_list):
    if "q" in request.GET:
        q = request.GET["q"]
        query_set = query_set.filter(Q(name__icontains=q) |
                                     Q(description__icontains=q))

    if "order_by" in request.GET:
        if request.GET["order_by"] == "updated":
            # mysql doesn't have the timestamp resolution needed to be
            # able to just filter by date modified during unit tests
            query_set = query_set.order_by("-date_modified", "-pk")
    else:
        query_set = query_set.order_by("pk")

    if page_list:
        query_set = _page_dataset_list(query_set, request)

    return query_set


def _page_dataset_list(query_set, request):
    if "page" in request.GET:
        page_size = 50
        if "page_size" in request.GET:
            page_size = int(request.GET["page_size"])

        page_num = int(request.GET["page"])
        start = (page_num - 1) * page_size
        end = start + page_size
        query_set = query_set[start:end]

    return query_set
