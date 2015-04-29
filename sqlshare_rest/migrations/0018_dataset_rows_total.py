# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0017_query_rows_total'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='rows_total',
            field=models.IntegerField(null=True),
            preserve_default=True,
        ),
    ]
