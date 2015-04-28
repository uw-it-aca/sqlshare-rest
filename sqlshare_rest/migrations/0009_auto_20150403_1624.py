# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('sqlshare_rest', '0008_auto_20150402_2244'),
    ]

    operations = [
        migrations.CreateModel(
            name='DatasetTag',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('dataset', models.ForeignKey(to='sqlshare_rest.Dataset')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Tag',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('tag', models.CharField(max_length=200, db_index=True)),
                ('popularity', models.IntegerField(default=0)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.AddField(
            model_name='datasettag',
            name='tag',
            field=models.ForeignKey(to='sqlshare_rest.Tag'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='datasettag',
            name='user',
            field=models.ForeignKey(to='sqlshare_rest.User'),
            preserve_default=True,
        ),
        migrations.AlterUniqueTogether(
            name='datasettag',
            unique_together=set([('tag', 'dataset')]),
        ),
    ]
