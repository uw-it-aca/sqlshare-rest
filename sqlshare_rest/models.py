from django.db import models
from django.core.urlresolvers import reverse
# from django_fields.fields import EncryptedCharField


class User(models.Model):
    """ A cached reference to a database user """
    username = models.CharField(max_length=200, db_index=True, unique=True)
    schema = models.CharField(max_length=200, unique=True)
    db_username = models.CharField(max_length=250, db_index=True, unique=True)
    # db_password = EncryptedCharField(max_length=200)
    db_password = models.CharField(max_length=200)


class Dataset(models.Model):
    """ A cached reference to a database view """
    name = models.CharField(max_length=200, db_index=True)
    owner = models.ForeignKey(User, db_index=True)
    sql = models.TextField(null=True)
    description = models.TextField(null=True)
    data_preview = models.TextField(null=True)
    is_public = models.BooleanField(default=False)
    is_shared = models.BooleanField(default=False)

    class Meta:
        unique_together = (("name", "owner"),)

    def json_data(self):
        return {
            "description": self.description,
            "is_public": self.is_public,
            "is_shared": self.is_shared,
            "sql_code": self.sql,
            "columns": None,
            "popularity": 0,
            "tags": [],
            "url": self.get_url(),
            "sample_data_status": "working",
        }

    def get_url(self):
        username = self.owner.username
        url = reverse("sqlshare_view_dataset", kwargs={'owner': username,
                                                       'name': self.name})
        return url

    def user_has_access(self, user):
        if user.username == self.owner.username:
            return True

        if self.is_public:
            return True

        return False
