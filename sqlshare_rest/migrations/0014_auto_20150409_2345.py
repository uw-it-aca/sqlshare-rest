# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0013_fileupload'),
    ]

    operations = [
        migrations.AddField(
            model_name='fileupload',
            name='dataset_description',
            field=models.TextField(null=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='fileupload',
            name='dataset_is_public',
            field=models.NullBooleanField(),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='fileupload',
            name='dataset_name',
            field=models.TextField(null=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='fileupload',
            name='error',
            field=models.TextField(null=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='fileupload',
            name='has_error',
            field=models.BooleanField(default=False),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='fileupload',
            name='is_finalized',
            field=models.NullBooleanField(db_index=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='fileupload',
            name='dataset_created',
            field=models.BooleanField(default=False, db_index=True),
            preserve_default=True,
        ),
    ]
