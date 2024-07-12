from django.urls import path
from .views import HomePageView, download_file

urlpatterns = [
    path('', HomePageView.as_view(), name='home'),
    path('process-csv/', HomePageView.as_view(), name='process_csv'),
    path('process-pdf/', HomePageView.as_view(), name='process_pdf'),
    path('download_file/<str:file_name>/', download_file, name='download_file'),
]
