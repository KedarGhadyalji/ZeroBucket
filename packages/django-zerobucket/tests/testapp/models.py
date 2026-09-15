"""A real Django model, used to prove ZeroBucketStorage works through
Django's actual ORM/FileField machinery -- not just called directly.
"""

from __future__ import annotations

from django.db import models

from django_zerobucket.storage import ZeroBucketStorage


class Product(models.Model):
    name = models.CharField(max_length=100)
    photo = models.ImageField(storage=ZeroBucketStorage, upload_to="ignored")

    class Meta:
        app_label = "testapp"
