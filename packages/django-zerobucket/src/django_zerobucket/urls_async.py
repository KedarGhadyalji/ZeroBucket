"""Like urls.py, but the route is served by AsyncServeImageView (see views.py).

Include this INSTEAD of "django_zerobucket.urls", not as well: both define the route
name "django_zerobucket:serve" that ZeroBucketStorage.url() reverses.

    urlpatterns = [
        ...,
        path("zerobucket-images/", include("django_zerobucket.urls_async")),
    ]
"""

from __future__ import annotations

from django.urls import path

from .views import AsyncServeImageView

app_name = "django_zerobucket"

urlpatterns = [
    path("<uuid:image_id>/", AsyncServeImageView.as_view(), name="serve"),
]
