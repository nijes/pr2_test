from django.db import models



class AvgTemp(models.Model):
    avgtempid = models.IntegerField(db_column='avgtempId', primary_key=True)  # Field name made lowercase.
    year = models.IntegerField()
    month = models.IntegerField()
    regionid = models.IntegerField(db_column='regionId')  # Field name made lowercase.
    avgtemp = models.FloatField(db_column='avgTemp', blank=True, null=True)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'avg_temp'


class CitygasCost(models.Model):
    costid = models.AutoField(db_column='costId', primary_key=True)  # Field name made lowercase.
    year = models.IntegerField(blank=True, null=True)
    month = models.IntegerField(blank=True, null=True)
    regionid = models.IntegerField(db_column='regionId')  # Field name made lowercase.
    citygascost = models.IntegerField(db_column='citygasCost', blank=True, null=True)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'citygas_cost'


class Demand(models.Model):
    demandid = models.AutoField(db_column='demandId', primary_key=True)  # Field name made lowercase.
    year = models.IntegerField(blank=True, null=True)
    month = models.IntegerField(blank=True, null=True)
    regionid = models.IntegerField(db_column='regionId')  # Field name made lowercase.
    demand = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'demand'


class Heatindex(models.Model):
    heatindexid = models.AutoField(db_column='heatIndexId', primary_key=True)  # Field name made lowercase.
    year = models.IntegerField(blank=True, null=True)
    month = models.IntegerField(blank=True, null=True)
    heatindex = models.FloatField(db_column='heatIndex', blank=True, null=True)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'heatindex'


class Household(models.Model):
    householdid = models.AutoField(db_column='householdId', primary_key=True)  # Field name made lowercase.
    year = models.IntegerField()
    regionid = models.IntegerField(db_column='regionId')  # Field name made lowercase.
    household = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'household'


class Importindex(models.Model):
    importindexid = models.AutoField(db_column='importIndexId', primary_key=True)  # Field name made lowercase.
    year = models.IntegerField(blank=True, null=True)
    month = models.IntegerField(blank=True, null=True)
    importpriceindex = models.FloatField(db_column='importPriceIndex', blank=True, null=True)  # Field name made lowercase.
    importamountindex = models.FloatField(db_column='importAmountIndex', blank=True, null=True)  # Field name made lowercase.
    importcostindex = models.FloatField(db_column='importCostIndex', blank=True, null=True)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'importindex'


class Region(models.Model):
    regionid = models.IntegerField(db_column='regionId', primary_key=True)  # Field name made lowercase.
    regionname = models.CharField(db_column='regionName', max_length=45)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'region'


class Relativeprice(models.Model):
    relativepriceid = models.AutoField(db_column='relativepriceId', primary_key=True)  # Field name made lowercase.
    year = models.IntegerField(blank=True, null=True)
    month = models.IntegerField(blank=True, null=True)
    gasproducerprice = models.FloatField(db_column='gasProducerPrice', blank=True, null=True)  # Field name made lowercase.
    oilproducerprice = models.FloatField(db_column='oilProducerPrice', blank=True, null=True)  # Field name made lowercase.
    relativeprice = models.FloatField(db_column='relativePrice', blank=True, null=True)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'relativeprice'


class Supply(models.Model):
    supplyid = models.AutoField(db_column='supplyId', primary_key=True)  # Field name made lowercase.
    year = models.IntegerField(blank=True, null=True)
    month = models.IntegerField(blank=True, null=True)
    regionid = models.IntegerField(db_column='regionId')  # Field name made lowercase.
    supply = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'supply'


class Lngimport(models.Model):
    lngimportid = models.AutoField(db_column='lngImportId', primary_key=True)  # Field name made lowercase.
    year = models.IntegerField(blank=True, null=True)
    import_field = models.IntegerField(db_column='import', blank=True, null=True)  # Field renamed because it was a Python reserved word.
    demand = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'lngimport'
