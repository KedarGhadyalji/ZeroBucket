"""An intentionally empty urlconf -- used by one test to confirm
ZeroBucketStorage.url() raises Django's normal NoReverseMatch when
django_zerobucket.urls hasn't been included anywhere."""

urlpatterns = []
