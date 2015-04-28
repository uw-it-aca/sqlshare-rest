# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0011_auto_20150406_1816'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='dataset',
            name='data_preview',
        ),
        migrations.AddField(
            model_name='dataset',
            name='preview_error',
            field=models.TextField(null=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='dataset',
            name='preview_is_finished',
            field=models.BooleanField(default=False),
            preserve_default=True,
        ),
    ]
