# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0014_auto_20150409_2345'),
    ]

    operations = [
        migrations.AlterField(
            model_name='fileupload',
            name='user_file',
            field=models.FileField(upload_to='user_files/%Y/%m/%d'),
            preserve_default=True,
        ),
    ]
