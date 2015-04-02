from django.db import models
from django.core.urlresolvers import reverse
from datetime import datetime
# from django_fields.fields import EncryptedCharField


class SharingEmail(models.Model):
    email = models.CharField(max_length=200)


class User(models.Model):
    """ A cached reference to a database user """
    username = models.CharField(max_length=200, db_index=True, unique=True)
    schema = models.CharField(max_length=200, unique=True)
    db_username = models.CharField(max_length=250, db_index=True, unique=True)
    # db_password = EncryptedCharField(max_length=200)
    db_password = models.CharField(max_length=200)

    def json_data(self):
        return {
            "login": self.username,
        }


class Dataset(models.Model):
    """ A cached reference to a database view """
    name = models.CharField(max_length=200, db_index=True)
    owner = models.ForeignKey(User, db_index=True)
    sql = models.TextField(null=True)
    description = models.TextField(null=True)
    data_preview = models.TextField(null=True)
    is_public = models.BooleanField(default=False)
    is_shared = models.BooleanField(default=False)
    shared_with = models.ManyToManyField(User, related_name="shared_with")
    email_shares = models.ManyToManyField(SharingEmail)
    date_created = models.DateTimeField(auto_now_add=True,
                                        default=datetime.now)
    date_modified = models.DateTimeField(auto_now=True, default=datetime.now)
    popularity = models.IntegerField(default=0)
    last_viewed = models.DateTimeField(null=True)

    class Meta:
        unique_together = (("name", "owner"),)

    def json_data(self):
        mod_date = self.date_modified.strftime("%a, %-d %b %Y %-H:%M:%S %Z")
        create_date = self.date_created.strftime("%a, %-d %b %Y %-H:%M:%S %Z")
        return {
            "name": self.name,
            "owner": self.owner.username,
            "description": self.description,
            "date_created": create_date,
            "date_modified": mod_date,
            "is_public": self.is_public,
            "is_shared": self.is_shared,
            "sql_code": self.sql,
            "columns": None,
            "popularity": self.popularity,
            "tags": [],
            "url": self.get_url(),
            "sample_data_status": "working",
        }

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
