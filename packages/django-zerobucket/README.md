# django-zerobucket

A Django [`Storage`](https://docs.djangoproject.com/en/stable/ref/files/storage/)
backend adapter for [ZeroBucket](https://github.com/KedarGhadyalji/ZeroBucket)
-- database-native image storage, plugged into Django's existing
`FileField`/`ImageField` machinery. No new field type to learn, works
with existing forms/admin/migrations the same way `django-storages`'
S3/GCS backends do for their targets.

## Installation

```bash
pip install django-zerobucket
```

Requires Django >= 5.2 and a PostgreSQL database ZeroBucket can store
images in (see the [core `zerobucket` package](https://github.com/KedarGhadyalji/ZeroBucket)
for schema/setup details -- this package doesn't change any of that,
it's a thin adapter on top).

## Setup

```python
# settings.py
INSTALLED_APPS = [
    ...,
    "django_zerobucket",   # needed for the zerobucket_* management commands
]

STORAGES = {
    "default": {
        "BACKEND": "django_zerobucket.storage.ZeroBucketStorage",
        "OPTIONS": {"database_url": "postgresql://user:pass@localhost/mydb"},
    },
}
# or, instead of OPTIONS above:
ZEROBUCKET_DATABASE_URL = "postgresql://user:pass@localhost/mydb"
```

```python
# project urls.py -- required for ZeroBucketStorage.url() to resolve
from django.urls import include, path

urlpatterns = [
    ...,
    path("images/", include("django_zerobucket.urls")),
]
```

```python
# models.py -- just a normal ImageField/FileField
class Product(models.Model):
    photo = models.ImageField(upload_to="ignored")
```

That's it. `product.photo.save(...)`, `product.photo.read()`,
`product.photo.url`, `product.photo.delete()` -- all work exactly like
any other Django Storage backend from here.

## The one thing worth knowing before you adopt this

**The `name` Django stores for each file is a ZeroBucket image id (a
UUID), not a filesystem-style path.** Every other Django Storage
backend builds a path out of `upload_to=` plus the uploaded filename
(e.g. `"uploads/2024/photo.jpg"`) -- ZeroBucket is _id-addressed_
storage, not path-addressed, so there's no "directory" for a file to
live in. `upload_to=` is still required by Django's field API but is
effectively ignored by this backend.

This is invisible for the vast majority of normal usage
(`instance.photo.url`, `.read()`, `.open()`, template `{{ instance.photo.url }}`
all work exactly the same) -- it only matters if your code inspects the
raw `instance.photo.name` string and expects something path-shaped. The
original upload filename isn't lost, though: it's preserved in
ZeroBucket's own metadata and retrievable via the core `zerobucket`
client's `metadata()`/`get()` methods, even though Django's own `name`
field won't show it.

## Serving images

`ZeroBucketStorage.url()` reverses to a built-in view
(`ServeImageView`) that streams the image back via ZeroBucket's own
`get_stream()` -- a real use of the core library's streaming feature,
not just a demonstration of it.

**No built-in access control.** This is a deliberate first-pass scope
decision: ZeroBucket's own `before_get`/`before_put` hooks exist for
exactly this, but wiring a `context=` through Django's request/auth
system into those hooks is a real design question of its own (whose
context -- the request? the user? something else?) that's out of scope
for this first pass. If you need access control on served images,
protect the view the normal Django way:

```python
from django.contrib.auth.decorators import login_required
from django_zerobucket.views import ServeImageView

urlpatterns = [
    path("images/<uuid:image_id>/",
         login_required(ServeImageView.as_view()),
         name="my_protected_image"),
]
```

(and don't also `include("django_zerobucket.urls")`, or the same images
stay reachable unprotected there too.)

Want images served some other way entirely -- a CDN in front of tiered
S3 objects, presigned S3 URLs for images you've moved via
`tier_to_object_storage()`, whatever -- subclass `ZeroBucketStorage` and
override `url()`. This backend has no special knowledge of tiering one
way or the other, since transparent reads mean it doesn't need any.

## Management commands

Thin wrappers around the core `zerobucket` CLI's already-tested
`cmd_info`/`cmd_verify`/`cmd_tier` -- not reimplementations. Same
behavior, same flags, just reading `ZEROBUCKET_DATABASE_URL` from
Django settings instead of requiring `--database-url`:

```bash
./manage.py zerobucket_info
./manage.py zerobucket_verify
./manage.py zerobucket_verify --sample 100
./manage.py zerobucket_tier IMAGE_ID --bucket my-bucket
./manage.py zerobucket_tier --all --bucket my-bucket
./manage.py zerobucket_tier --min-size 2000000 --older-than 90 --bucket my-bucket
```

`zerobucket_tier` needs `boto3` (`pip install "zerobucket[s3]"`) the
same way the standalone `zerobucket tier` CLI command does -- not
pulled in by installing `django-zerobucket` itself. `zerobucket_verify`/
`zerobucket_tier` exit non-zero on failure (checksum mismatch, tier
failure), same convention the standalone CLI uses, so both are usable
in a cron job or CI/deploy step, not just interactively.

## What this package deliberately doesn't do (yet)

- **No custom model field.** A Storage adapter was judged the higher-
  leverage, more idiomatic choice for a first pass -- it composes with
  Django's existing `FileField`/`ImageField`, forms, and admin, rather
  than asking every adopting project to learn a new field type.
- **No async support.** The core `zerobucket` package ships
  `AsyncZeroBucket`; this package's `ZeroBucketStorage`/`ServeImageView`
  are both built on the sync client. Django's own async views/ORM paths
  aren't wired up to it yet.
- **No signal-based cleanup.** Django does not automatically delete
  storage files when a model instance is deleted (this is a general
  Django behavior, not specific to this backend -- confirmed directly,
  not assumed) -- if you want images cleaned up when their owning model
  is deleted, add a `post_delete` signal calling `instance.photo.delete()`
  yourself, same as you would with any other Django Storage backend.
- **No `dedup=True`/tiering-aware URLs.** Both are fully supported at
  the core `zerobucket` client level (pass the relevant options through
  `STORAGES`' `OPTIONS`) and work transparently through this adapter --
  there's just no Django-specific UI/behavior built on top of them yet
  (e.g. an admin action to bulk-tier selected objects).

## Development

```bash
cd packages/django-zerobucket
pip install -e ".[dev]"
export ZEROBUCKET_TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/zerobucket_test
pytest -q
ruff check src tests
```

Tests run against a real PostgreSQL instance, used for two separate
roles simultaneously: Django's own `DATABASES` (its internal tables plus
this test suite's dummy `Product` model) and `ZEROBUCKET_DATABASE_URL`
(where ZeroBucket actually stores image bytes) -- see
`tests/django_settings.py`'s module docstring for why those are kept
conceptually distinct even when pointed at the same physical server in
this test setup.

## License

MIT, same as the core `zerobucket` package.
