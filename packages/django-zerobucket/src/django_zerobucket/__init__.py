"""django-zerobucket: a Django Storage backend adapter for ZeroBucket.

    from django_zerobucket import ZeroBucketStorage

See storage.py's module docstring for the id-vs-path naming difference
from typical Django Storage backends, and views.py's for how served
images work and their access-control scope.

default_app_config is not needed (Django's app-loading auto-discovery
handles apps.py without it since Django 3.2) -- this package still
needs "django_zerobucket" added to INSTALLED_APPS for its management
commands (management/commands/zerobucket_*.py) to be discovered, even
though the Storage backend and serving view themselves would technically
work without that (Django's STORAGES/urlconf wiring doesn't require
INSTALLED_APPS membership, only manage.py's command discovery does).
"""

from __future__ import annotations

from .storage import ZeroBucketStorage

__version__ = "0.1.0"

__all__ = ["ZeroBucketStorage"]
