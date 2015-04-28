# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0004_auto_20150320_2330'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='shared_with',
            field=models.ManyToManyField(related_name='shared_with', to='sqlshare_rest.User'),
            preserve_default=True,
        ),
    ]
