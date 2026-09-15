from django.urls import include, path

urlpatterns = [
    path("zerobucket-images/", include("django_zerobucket.urls")),
]
