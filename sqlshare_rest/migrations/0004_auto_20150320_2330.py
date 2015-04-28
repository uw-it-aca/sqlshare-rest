# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0003_dataset'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dataset',
            name='name',
            field=models.CharField(max_length=200, db_index=True),
            preserve_default=True,
        ),
        migrations.AlterUniqueTogether(
            name='dataset',
            unique_together=set([('name', 'owner')]),
        ),
    ]
