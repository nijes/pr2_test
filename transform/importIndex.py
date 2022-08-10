from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, substring, asc

sc = SparkContext()
spark =SparkSession.builder.getOrCreate()

user="root"
password="1234"
url="jdbc:mysql://localhost:3306/phoenix"
driver="com.mysql.cj.jdbc.Driver"
dbtable="importIndex" #table 이름 맞게 지정


###### 수입금액지수, 수입물량지수, 수입금액지수 각각에 대한 데이터프레임############

df = spark.read.json('import_price1.json')
df_explode = df.select(explode(df.StatisticSearch.row))
import_price1 = df_explode.select(df_explode.col.TIME, df_explode.col.ITEM_NAME1, df_explode.col.DATA_VALUE)\
.withColumnRenamed('col.Time', 'time')\
.withColumnRenamed('col.ITEM_NAME1', 'item')\
.withColumnRenamed('col.DATA_VALUE', 'importPriceIndex')\
.where(col('item')=='원유및천연가스')\
.where(col('col.ITEM_NAME2')=='계약통화기준')
#import_price1.show()

df = spark.read.json('import_price2.json')
df_explode = df.select(explode(df.StatisticSearch.row))
import_price2 = df_explode.select(df_explode.col.TIME, df_explode.col.ITEM_NAME1, df_explode.col.DATA_VALUE)\
.withColumnRenamed('col.Time', 'time')\
.withColumnRenamed('col.ITEM_NAME1', 'item')\
.withColumnRenamed('col.DATA_VALUE', 'importPriceIndex')\
.where(col('item')=='원유및천연가스')\
.where(col('col.ITEM_NAME2')=='계약통화기준')
#import_price2.show()

import_price = import_price1.union(import_price2).drop('item')



df = spark.read.json('import_amount1.json')
df_explode = df.select(explode(df.StatisticSearch.row))
import_amount1 = df_explode.select(df_explode.col.TIME, df_explode.col.ITEM_NAME1, df_explode.col.DATA_VALUE)\
.withColumnRenamed('col.Time', 'time')\
.withColumnRenamed('col.ITEM_NAME1', 'item')\
.withColumnRenamed('col.DATA_VALUE', 'importAmountIndex')\
.where(col('item')=='천연가스(LNG)')
#import_amount1.show()
#LNG의 수입물량지수는 20000년 1월부터 기록

df = spark.read.json('import_amount2.json')
df_explode = df.select(explode(df.StatisticSearch.row))
import_amount2 = df_explode.select(df_explode.col.TIME, df_explode.col.ITEM_NAME1, df_explode.col.DATA_VALUE)\
.withColumnRenamed('col.Time', 'time')\
.withColumnRenamed('col.ITEM_NAME1', 'item')\
.withColumnRenamed('col.DATA_VALUE', 'importAmountIndex')\
.where(col('item')=='천연가스(LNG)')
#import_amount2.show()

import_amount = import_amount1.union(import_amount2).drop('item')



df = spark.read.json('import_cost1.json')
df_explode = df.select(explode(df.StatisticSearch.row))
import_cost1 = df_explode.select(df_explode.col.TIME, df_explode.col.ITEM_NAME1, df_explode.col.DATA_VALUE)\
.withColumnRenamed('col.Time', 'time')\
.withColumnRenamed('col.ITEM_NAME1', 'item')\
.withColumnRenamed('col.DATA_VALUE', 'importCostIndex')\
.where(col('item')=='천연가스(LNG)')
#import_cost1.show()
#LNG의 수입물가지수는 20000년 1월부터 기록

df = spark.read.json('import_cost2.json')
df_explode = df.select(explode(df.StatisticSearch.row))
import_cost2 = df_explode.select(df_explode.col.TIME, df_explode.col.ITEM_NAME1, df_explode.col.DATA_VALUE)\
.withColumnRenamed('col.Time', 'time')\
.withColumnRenamed('col.ITEM_NAME1', 'item')\
.withColumnRenamed('col.DATA_VALUE', 'importCostIndex')\
.where(col('item')=='천연가스(LNG)')
#import_cost2.show()

import_cost = import_cost1.union(import_cost2).drop('item')


###### 3개의 데이터프레임 조인해서 하나의 데이터프레임으로 ###################

ym_lst = []
k = 0
for i in range(1988, 2023):
    for j in range(1, 13):
        k += 1
        yyyymm = str(i) + str(j).zfill(2)
        ym_lst.append((k, yyyymm))

ym = sc.parallelize(ym_lst).toDF(['importIndexId', 'time_st']) #연월 기준이 될 데이터프레임
#ym.show()

join_df = ym.join(import_price, ym.time_st == import_price.time, 'left_outer')\
.join(import_amount, ym.time_st == import_amount.time, 'left_outer')\
.join(import_cost, ym.time_st == import_cost.time, 'left_outer')

insert_df = join_df.select(col('importIndexId'), substring('time_st', 1, 4).alias('year')\
, substring('time_st', 5, 2).alias('month')\
, col('importPriceIndex'), col('importAmountIndex'), col('importCostIndex'))\
.drop('time').orderBy(asc('importIndexId'))



#######################################################


insert_df.write.jdbc(url, dbtable, "append", properties={"driver": driver, "user": user, "password": password})

