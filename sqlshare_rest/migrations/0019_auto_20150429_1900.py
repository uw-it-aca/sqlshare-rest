# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0018_dataset_rows_total'),
    ]

    operations = [
        migrations.AddField(
            model_name='datasetsharingemail',
            name='date_sent',
            field=models.DateTimeField(null=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='datasetsharingemail',
            name='email_sent',
            field=models.BooleanField(default=False),
            preserve_default=True,
        ),
    ]
