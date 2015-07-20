from django.db import models
from django.core.urlresolvers import reverse
from datetime import datetime
from django.utils import timezone
from oauth2client.django_orm import CredentialsField, FlowField
import json
from sqlshare_rest.util.queue_triggers import trigger_query_queue_processing
import uuid
import six
from six import add_metaclass
from dirtyfields import DirtyFieldsMixin
# from django_fields.fields import EncryptedCharField

JSON_DATE = "%a, %-d %b %Y %-H:%M:%S %Z"


class User(models.Model):
    """ A cached reference to a database user """
    username = models.CharField(max_length=200, db_index=True, unique=True)
    schema = models.CharField(max_length=200, unique=True)
    db_username = models.CharField(max_length=250, db_index=True, unique=True)
    # db_password = EncryptedCharField(max_length=200)
    db_password = models.CharField(max_length=200)
    override_as = models.ForeignKey("User", null=True)

    def get_full_name(self):
        # TODO
        return ""

    def json_data(self):
        return {
            "login": self.username,
        }


class Dataset(DirtyFieldsMixin, models.Model):
    """ A cached reference to a database view """
    name = models.CharField(max_length=200, db_index=True)
    owner = models.ForeignKey(User, db_index=True)
    sql = models.TextField(null=True)
    description = models.TextField(null=True)
    is_public = models.BooleanField(default=False)
    is_shared = models.BooleanField(default=False)
    shared_with = models.ManyToManyField(User, related_name="shared_with")
    date_created = models.DateTimeField(auto_now_add=True,
                                        default=timezone.now)
    date_modified = models.DateTimeField(default=timezone.now)
    popularity = models.IntegerField(default=0)
    last_viewed = models.DateTimeField(null=True)
    preview_is_finished = models.BooleanField(default=False)
    preview_error = models.TextField(null=True)
    rows_total = models.IntegerField(null=True)
    snapshot_source = models.ForeignKey('Dataset', null=True)
    snapshot_finished = models.NullBooleanField()

    class Meta:
        unique_together = (("name", "owner"),)

    def json_data(self):
        mod_date = self.date_modified.strftime(JSON_DATE)
        create_date = self.date_created.strftime(JSON_DATE)
        upload_errors = self.get_upload_errors()

        description = self.description
        if not description:
            description = ""

        return {
            "name": self.name,
            "owner": self.owner.username,
            "description": description,
            "date_created": create_date,
            "date_modified": mod_date,
            "is_public": self.is_public,
            "is_shared": self.is_shared,
            "sql_code": self.sql,
            "columns": None,
            "popularity": self.popularity,
            "tags": self.get_tags_data(),
            "url": self.get_url(),
            "sample_data_status": self.get_sample_data_status(),
            "sample_data_error": self.preview_error,
            "rows_total": self.rows_total,
            "upload_errors": upload_errors,
        }

    def get_upload_errors(self):
        try:
            file_uploads = FileUpload.objects.filter(dataset__pk=self.pk)
            if file_uploads:
                return file_uploads[0].error
        except FileUpload.DoesNotExist:
            return ""
        return ""

    def get_sample_data_status(self):
        if self.preview_is_finished and not self.preview_error:
            return "success"
        elif not self.preview_is_finished:
            return "working"
        else:
            return "error"

    def get_tags_data(self):
        by_user = []
        filtered = DatasetTag.objects.filter(dataset=self)
        tags = filtered.order_by('user__username', 'tag__tag')

        current_user = None
        for tag in tags:
            username = tag.user.username
            if username != current_user:
                current_user = username
                by_user.append({"name": username, "tags": []})

            by_user[len(by_user)-1]["tags"].append(tag.tag.tag)

        return by_user

    def get_url(self):
        username = self.owner.username
        url = reverse("sqlshare_view_dataset", kwargs={'owner': username,
                                                       'name': self.name})
        return url

    def user_has_read_access(self, user):
        if user.username == self.owner.username:
            return True

        if self.is_public:
            return True

        if self.is_shared:
            try:
                ss_user = User.objects.get(username=user.username)
            except User.DoesNotExist:
                return False

            val = self.shared_with.filter(pk=ss_user.pk).exists()
            return val

        return False

    def should_update_modified_date(self, field_name):
        modify_fields = {
            "description": True,
            "is_public": True,
            "is_shared": True,
            "sql": True,
            "shared_with": True,
        }

        if field_name in modify_fields:
            return True
        return False

    def save(self, *args, **kwargs):
        dirty_fields = self.get_dirty_fields()
        should_update = False
        for field in dirty_fields:
            if self.should_update_modified_date(field):
                should_update = True

        if should_update:
            self.date_modified = timezone.now()
        super(Dataset, self).save(*args, **kwargs)


class RecentDatasetView(models.Model):
    dataset = models.ForeignKey(Dataset)
    user = models.ForeignKey(User)
    timestamp = models.DateTimeField(null=True)

    class Meta:
        unique_together = (('dataset', 'user'),)


class SharingEmail(models.Model):
    email = models.CharField(max_length=200)


class DatasetSharingEmail(models.Model):
    """
    Connects a dataset with the sharing email.
    This level is needed, so a unique per dataset/email access token
    can be created.
    """
    email = models.ForeignKey(SharingEmail)
    dataset = models.ForeignKey(Dataset)
    access_token = models.CharField(max_length=100, null=True)
    email_sent = models.BooleanField(default=False)
    date_sent = models.DateTimeField(null=True)

    def generate_token(self):
        return uuid.uuid4().hex

    def save(self, *args, **kwargs):
        if not self.access_token:
            self.access_token = self.generate_token()

        super(DatasetSharingEmail, self).save(*args, **kwargs)


class Tag(models.Model):
    """ A tag for datasets, with popularity """
    tag = models.CharField(max_length=200, db_index=True)
    popularity = models.IntegerField(default=0)


class DatasetTag(models.Model):
    tag = models.ForeignKey(Tag)
    user = models.ForeignKey(User)
    dataset = models.ForeignKey(Dataset, db_index=True)

    class Meta:
        unique_together = (("tag", "dataset"),)


class Query(models.Model):
    sql = models.TextField(null=True)
    is_finished = models.BooleanField(default=False)
    is_started = models.BooleanField(default=False)
    has_error = models.BooleanField(default=False)
    error = models.TextField(null=True)
    is_preview_for = models.ForeignKey(Dataset, null=True)
    owner = models.ForeignKey(User)
    date_created = models.DateTimeField(auto_now_add=True,
                                        default=timezone.now)
    date_finished = models.DateTimeField(null=True)
    rows_total = models.IntegerField(null=True)
    process_queue_id = models.IntegerField(null=True)
    terminated = models.BooleanField(default=False)
    preview_content = models.TextField(null=True)
    query_plan = models.TextField(null=True)
    query_time = models.FloatField(null=True)
    total_time = models.FloatField(null=True)

    def save(self, *args, **kwargs):
        super(Query, self).save(*args, **kwargs)
        trigger_query_queue_processing()

    def json_data(self, request):
        """
        Needs to run a query as the current user, which comes from the request.
        """

        finish_date = None
        if self.date_finished:
            finish_date = self.date_finished.strftime(JSON_DATE)
        create_date = self.date_created.strftime(JSON_DATE)

        return {
            "sql_code": self.sql,
            "is_finished": self.is_finished,
            "has_error": self.has_error,
            "error": self.error,
            "date_created": create_date,
            "date_finished": finish_date,
            "url": self.get_url(),
            "sample_data_status": self.get_sample_data_status(),
            "sample_data": None,  # Comes in at the view level
            "columns": None,  # Comes in at the view level
            "rows_total": self.rows_total,
        }

    def get_sample_data_status(self):
        if self.is_finished and not self.error:
            return "success"
        elif not self.is_finished:
            return "working"
        else:
            return "error"

    def get_url(self):
        return reverse("sqlshare_view_query", kwargs={'id': self.pk})


class FileUpload(models.Model):
    MAX_PARSER_PREVIEW = 50
    owner = models.ForeignKey(User, db_index=True)
    has_parser_values = models.BooleanField(default=False)
    has_column_header = models.NullBooleanField(null=True)
    delimiter = models.CharField(null=True, max_length=5)
    column_list = models.TextField(null=True)
    sample_data = models.TextField(null=True)
    user_file = models.FileField(upload_to="user_files/%Y/%m/%d")
    date_created = models.DateTimeField(auto_now_add=True,
                                        default=timezone.now)
    dataset_created = models.BooleanField(default=False, db_index=True)
    dataset = models.ForeignKey(Dataset, null=True)
    has_error = models.BooleanField(default=False)
    error = models.TextField(null=True)
    dataset_name = models.TextField(null=True)
    dataset_description = models.TextField(null=True)
    dataset_is_public = models.NullBooleanField()
    is_started = models.BooleanField(default=False)
    is_finalized = models.NullBooleanField(db_index=True)
    rows_total = models.IntegerField(null=True)
    rows_loaded = models.IntegerField(null=True)

    def finalize_json_data(self):
        return {
            "rows_total": self.rows_total,
            "rows_loaded": self.rows_loaded,
        }

    def parser_json_data(self):
        column_data = None
        column_list = json.loads(self.column_list)
        if column_list:
            column_data = list(map(lambda x: {"name": x},
                                   column_list))

        max_rows = FileUpload.MAX_PARSER_PREVIEW
        return {
            "parser": {"delimiter": self.delimiter,
                       "has_column_headers": self.has_column_header},
            "columns": column_data,
            "sample_data": json.loads(self.sample_data)[:max_rows]
        }


if six.PY3:
    # Python3 shims.
    # Still need to use add_metaclass, so the python2 parser doesn't break
    # on metaclass=...

    # But - in python2 this breaks.  so double shimmed.
    @add_metaclass(models.SubfieldBase)
    class Py3FlowField(FlowField):
        pass

    @add_metaclass(models.SubfieldBase)
    class Py3CredentialsField(CredentialsField):
        pass

if six.PY2:
    class Py3FlowField(FlowField):
        pass

    class Py3CredentialsField(CredentialsField):
        pass


# These are for the google logins
class CredentialsModel(models.Model):
    id = models.CharField(max_length=50, primary_key=True)
    credential = Py3CredentialsField()


class FlowModel(models.Model):
    id = models.CharField(max_length=50, primary_key=True)
    flow = Py3FlowField()


class DownloadToken(models.Model):
    token = models.CharField(max_length=32)
    sql = models.TextField()
    original_user = models.ForeignKey(User)

    def _generate_token(self):
        return uuid.uuid4().hex

    def store_token_for_sql(self, sql, user):
        self.sql = sql
        self.original_user = user
        self.token = self._generate_token()
        self.save()

    def validate_token(self, token):
        # Deletes token on validation to ensure single-use
        token = DownloadToken.objects.get(token=token)
        token.delete()
        return token


class LogoutToken(models.Model):
    token = models.CharField(max_length=32)
    user = models.ForeignKey(User)
    date_created = models.DateTimeField(default=timezone.now)

    def _generate_token(self):
        return uuid.uuid4().hex

    def store_token_for_user(self, user):
        self.user = user
        self.token = self._generate_token()
        self.save()
