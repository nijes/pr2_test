from django.db import models


class Region(models.Model):
    regionid = models.IntegerField(db_column='regionId', primary_key=True)  # Field name made lowercase.
    regionname = models.CharField(db_column='regionName', max_length=45)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'region'
