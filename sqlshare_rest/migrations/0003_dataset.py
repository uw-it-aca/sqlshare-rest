# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0002_auto_20150320_2323'),
    ]

    operations = [
        migrations.CreateModel(
            name='Dataset',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=200)),
                ('sql', models.TextField(null=True)),
                ('description', models.TextField(null=True)),
                ('data_preview', models.TextField(null=True)),
                ('is_public', models.BooleanField(default=False)),
                ('is_shared', models.BooleanField(default=False)),
                ('owner', models.ForeignKey(to='sqlshare_rest.User')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
    ]
