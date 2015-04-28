# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import datetime


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0009_auto_20150403_1624'),
    ]

    operations = [
        migrations.CreateModel(
            name='Query',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('sql', models.TextField(null=True)),
                ('is_finished', models.BooleanField(default=False)),
                ('has_error', models.BooleanField(default=False)),
                ('error', models.TextField(null=True)),
                ('date_created', models.DateTimeField(default=datetime.datetime.now, auto_now_add=True)),
                ('date_finished', models.DateTimeField(null=True)),
                ('is_preview_for', models.ForeignKey(to='sqlshare_rest.Dataset', null=True)),
                ('owner', models.ForeignKey(to='sqlshare_rest.User')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
    ]
