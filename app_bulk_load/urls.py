from django.urls import path
from .views import all_vouchers_upload_excel

urlpatterns = [
    path('upload/excel/all-vouchers', all_vouchers_upload_excel, name='all_vouchers'),
]