# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0016_auto_20150424_1603'),
    ]

    operations = [
        migrations.AddField(
            model_name='query',
            name='rows_total',
            field=models.IntegerField(null=True),
            preserve_default=True,
        ),
    ]
