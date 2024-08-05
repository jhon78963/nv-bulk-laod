from django.db import models

# Create your models here.
class All_vouchers(models.Model):
    grade = models.CharField(max_length=30)
    student = models.CharField(max_length=255)
    description = models.CharField(max_length=255)
    voucher = models.CharField(max_length=255)
    amount = models.CharField(max_length=255)
    date = models.CharField(max_length=255)
    no_operation = models.CharField(max_length=255, null=True, blank=True)