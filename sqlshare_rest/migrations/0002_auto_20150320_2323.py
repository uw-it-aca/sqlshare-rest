# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0001_initial'),
    ]

    operations = [
        migrations.DeleteModel(
            name='Dataset',
        ),
        migrations.AlterField(
            model_name='user',
            name='db_username',
            field=models.CharField(unique=True, max_length=250, db_index=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='user',
            name='schema',
            field=models.CharField(unique=True, max_length=200),
            preserve_default=True,
        ),
    ]
