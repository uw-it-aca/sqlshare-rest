# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='user',
            name='db_password',
            field=models.CharField(max_length=200),
        ),
    ]
