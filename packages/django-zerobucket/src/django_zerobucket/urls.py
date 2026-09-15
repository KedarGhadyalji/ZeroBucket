"""Include this in your project's urlconf for ZeroBucketStorage.url()
to resolve -- see views.py's module docstring for the access-control
note (no built-in auth; wrap ServeImageView yourself if you need it).

    urlpatterns = [
        ...,
        path("zerobucket-images/", include("django_zerobucket.urls")),
    ]

The path prefix ("zerobucket-images/" above) is entirely up to you --
ZeroBucketStorage.url() only cares about the reverse() name
("django_zerobucket:serve"), not the actual URL shape, so you can mount
this wherever fits your project.
"""

from __future__ import annotations

from django.urls import path

from .views import ServeImageView

app_name = "django_zerobucket"

urlpatterns = [
    path("<uuid:image_id>/", ServeImageView.as_view(), name="serve"),
]
