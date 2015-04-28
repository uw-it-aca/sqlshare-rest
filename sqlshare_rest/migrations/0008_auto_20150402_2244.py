# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0007_auto_20150402_2036'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='last_viewed',
            field=models.DateTimeField(null=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='dataset',
            name='popularity',
            field=models.IntegerField(default=0),
            preserve_default=True,
        ),
    ]
