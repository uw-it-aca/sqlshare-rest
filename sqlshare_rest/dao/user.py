from sqlshare_rest.util.db import get_backend


def get_original_user(request):
    base = request.user
    user = get_backend().get_user(base.username)
    return user


def get_override_user(request):
    original = get_original_user(request)
    return original.override_as


def clear_override(request):
    original = get_original_user(request)
    original.override_as = None
    original.save()


def set_override_user(request, username):
    user = get_backend().get_user(username)

    original = get_original_user(request)
    original.override_as = user
    original.save()


def get_user(request):
    original = get_original_user(request)

    if original.override_as:
        return original.override_as

    return original
