# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import datetime


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0006_auto_20150401_1850'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='date_created',
            field=models.DateTimeField(default=datetime.datetime.now, auto_now_add=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='dataset',
            name='date_modified',
            field=models.DateTimeField(default=datetime.datetime.now, auto_now=True),
            preserve_default=True,
        ),
    ]
