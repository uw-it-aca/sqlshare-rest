# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0012_auto_20150408_2208'),
    ]

    operations = [
        migrations.CreateModel(
            name='FileUpload',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('has_parser_values', models.BooleanField(default=False)),
                ('has_column_header', models.NullBooleanField()),
                ('delimiter', models.CharField(max_length=5, null=True)),
                ('column_list', models.TextField(null=True)),
                ('sample_data', models.TextField(null=True)),
                ('user_file', models.FileField(upload_to=b'user_files/%Y/%m/%d')),
                ('date_created', models.DateTimeField(default=django.utils.timezone.now, auto_now_add=True)),
                ('dataset_created', models.BooleanField(default=False)),
                ('dataset', models.ForeignKey(to='sqlshare_rest.Dataset', null=True)),
                ('owner', models.ForeignKey(to='sqlshare_rest.User')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
    ]
