# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0005_dataset_shared_with'),
    ]

    operations = [
        migrations.CreateModel(
            name='SharingEmail',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('email', models.CharField(max_length=200)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.AddField(
            model_name='dataset',
            name='email_shares',
            field=models.ManyToManyField(to='sqlshare_rest.SharingEmail'),
            preserve_default=True,
        ),
    ]
