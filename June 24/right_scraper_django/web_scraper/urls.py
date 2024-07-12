from django.urls import path
from . import views

urlpatterns = [
    # Other URL patterns (if any) go here
    path('', views.home, name='home'),
]