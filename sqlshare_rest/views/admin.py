from django.http import HttpResponse
from django.contrib.auth.decorators import login_required
import logging
from django.conf import settings
from sqlshare_rest.dao.user import get_original_user, get_override_user
from sqlshare_rest.dao.user import clear_override, set_override_user
from authz_group import Group
from django import template
from django.shortcuts import render_to_response, render
from django.template import RequestContext
from sqlshare_rest.models import User, Dataset
from django.db.models import Count


def require_admin(decorated):
    @login_required
    def wrapped(request, *args, **kwargs):
        if not hasattr(settings, "USERSERVICE_ADMIN_GROUP"):
            print("You must have a group defined as your admin group.")
            print('Configure that using USERSERVICE_ADMIN_GROUP="foo_group"')
            raise Exception("Missing USERSERVICE_ADMIN_GROUP in settings")

        actual_user = get_original_user(request)
        if not actual_user:
            raise Exception("No user in session")

        g = Group()
        group_name = settings.USERSERVICE_ADMIN_GROUP
        is_admin = g.is_member_of_group(actual_user.username, group_name)
        if not is_admin:
            return render_to_response('no_access.html', {})

        return decorated(request, *args, **kwargs)

    return wrapped


"""
This is basically a rip-off os https://github.com/uw-it-aca/django-userservice

But - we can't work in the session, so this stashed the override in the db
by user.
"""


@require_admin
def user_override(request):
    # timer = Timer()
    logger = logging.getLogger(__name__)

    override_error_username = None
    override_error_msg = None
    # Do the group auth here.

    if "override_as" in request.POST:
        new_user = request.POST["override_as"].strip()
        validation_module = _get_validation_module()
        validation_error = validation_module(new_user)
        if validation_error is None:
            logger.info("%s is impersonating %s",
                        get_original_user(request).username,
                        new_user)
            set_override_user(request, new_user)
        else:
            override_error_username = new_user
            override_error_msg = validation_error

    if "clear_override" in request.POST:
        override_username = "<none>"
        override_obj = get_override_user(request)
        if override_obj:
            override_username = override_obj.username
        logger.info("%s is ending impersonation of %s",
                    get_original_user(request).username,
                    override_username)
        clear_override(request)

    override_user = get_override_user(request)
    if override_user:
        override_username = override_user.username
    else:
        override_username = None
    context = {
        'original_user': get_original_user(request).username,
        'override_user': override_username,
        'override_error_username': override_error_username,
        'override_error_msg': override_error_msg,
    }

    add_template_context(context)

    return render(request, 'admin/user_override.html', context)


@require_admin
def stats(request):
    context = {
        'users': [],
        'datasets': [],
    }

    users = User.objects.annotate(num_datasets=Count('dataset', distinct=True),
                                  num_queries=Count('query', distinct=True))
    users = users.order_by('num_datasets', 'num_queries')

    for u in users:
        context['users'].append({'username': u.username,
                                 'datasets': u.num_datasets,
                                 'queries': u.num_queries,
                                 })

    top_datasets = User.objects.annotate(num_datasets=Count('dataset',
                                                            distinct=True))
    top_datasets = top_datasets.order_by('-num_datasets')[:10]
    context['top_dataset_users'] = top_datasets

    top_queries = User.objects.annotate(num_queries=Count('query',
                                                          distinct=True))
    top_queries = top_queries.order_by('-num_queries')[:10]
    context['top_query_users'] = top_queries

    public_count = 0
    shared_count = 0
    datasets = Dataset.objects.annotate(Count('datasetsharingemail',
                                              distinct=True),
                                        Count('shared_with', distinct=True))
    for d in datasets:
        if d.is_public:
            public_count += 1
        elif d.is_shared:
            shared_count += 1
            context['datasets'].append({'name': d.name,
                                        'emails': d.datasetsharingemail__count,
                                        'accounts': d.shared_with__count})

    context['total_datasets'] = len(datasets)
    context['public_datasets'] = public_count
    context['shared_datasets'] = shared_count

    add_template_context(context)
    return render(request, "admin/stats.html", context)


def add_template_context(context):
    try:
        template_name = "userservice/user_override_extra_info.html"
        template.loader.get_template(template_name)
        context['has_extra_template'] = True
        context['extra_template'] = template_name
    except template.TemplateDoesNotExist:
        # This is a fine exception - there doesn't need to be an extra info
        # template
        pass

    try:
        template.loader.get_template("userservice/user_override_wrapper.html")
        context['wrapper_template'] = 'userservice/user_override_wrapper.html'
    except template.TemplateDoesNotExist:
        # This is a fine exception - there doesn't need to be an extra info
        # template
        context['wrapper_template'] = 'support_wrapper.html'


def _get_validation_module():
    if hasattr(settings, "USERSERVICE_VALIDATION_MODULE"):
        base = getattr(settings, "USERSERVICE_VALIDATION_MODULE")
        module, attr = base.rsplit('.', 1)
        try:
            mod = import_module(module)
        except ImportError as e:
            raise ImproperlyConfigured('Error importing module %s: "%s"' %
                                       (module, e))
        try:
            validation_module = getattr(mod, attr)
        except AttributeError:
            raise ImproperlyConfigured('Module "%s" does not define a '
                                       '"%s" class' % (module, attr))
        return validation_module
    else:
        return validate


def validate(username):
    error_msg = "No override user supplied"
    if (len(username) > 0):
        error_msg = None
    return error_msg
