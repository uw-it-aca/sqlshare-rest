# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0015_auto_20150422_1737'),
    ]

    operations = [
        migrations.CreateModel(
            name='DatasetSharingEmail',
            fields=[
                ('id', models.AutoField(auto_created=True, serialize=False, primary_key=True, verbose_name='ID')),
                ('access_token', models.CharField(max_length=100, null=True)),
                ('dataset', models.ForeignKey(to='sqlshare_rest.Dataset')),
                ('email', models.ForeignKey(to='sqlshare_rest.SharingEmail')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.RemoveField(
            model_name='dataset',
            name='email_shares',
        ),
    ]
