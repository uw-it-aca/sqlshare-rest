from django.db import models
from django_fields.fields import EncryptedCharField


class User(models.Model):
    """ A cached reference to a database user """
    username = models.CharField(max_length=200, db_index=True, unique=True)
    schema = models.CharField(max_length=200)
    db_username = models.CharField(max_length=250)
    db_password = EncryptedCharField(max_length=200)


class Dataset(models.Model):
    """ A cached reference to a database view """
    pass
