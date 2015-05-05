# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import sqlshare_rest.models


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0019_auto_20150429_1900'),
    ]

    operations = [
        migrations.CreateModel(
            name='CredentialsModel',
            fields=[
                ('id', models.CharField(primary_key=True, serialize=False, max_length=50)),
                ('credential', sqlshare_rest.models.Py3CredentialsField(null=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='FlowModel',
            fields=[
                ('id', models.CharField(primary_key=True, serialize=False, max_length=50)),
                ('flow', sqlshare_rest.models.Py3FlowField(null=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
    ]
