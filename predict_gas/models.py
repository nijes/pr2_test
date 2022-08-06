from django.db import models


class region2(models.Model):
    regionId = models.CharField(max_length=100)
    regionName = models.CharField(max_length=500)