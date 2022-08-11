

from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession.builder.getOrCreate()

user="root"
password="1234"
url="jdbc:mysql://localhost:3306/phoenix"
driver="com.mysql.cj.jdbc.Driver"
dbtable="relativeprice"

producer1 = spark.read.json('/predict_gas/raw_data/producer_price/producer_price1.json')
producer1.show()

from pyspark.sql.functions import explode, col
producer_explode1 = producer1.select(explode(producer1['StatisticSearch']['row']))
producer_df1 = producer_explode1.select(producer_explode1.col['TIME'], producer_explode1.col['ITEM_NAME1'], producer_explode1.col['DATA_VALUE'])

producer_df1.createOrReplaceTempView('producer_df1')

#  테이블
oil = producer_df1.where(col('col.ITEM_NAME1') == '원유정제처리제품')\
.withColumnRenamed('col.TIME', 'oildate')\
.withColumnRenamed('col.DATA_VALUE', 'oilProducerPrice').drop('col.ITEM_NAME1')

# 도시가스 테이블
citygas = producer_df1.where(col('col.ITEM_NAME1') == '도시가스')\
.dropDuplicates(['col.TIME']).withColumnRenamed('col.TIME', 'date')\
.withColumnRenamed('col.DATA_VALUE', 'gasProducerPrice').drop('col.ITEM_NAME1')

citygas.count() # 398
oil.count() # 397

citygas.createOrReplaceTempView('citygas')
oil.createOrReplaceTempView('oil')
# left join, 날짜 한 열 삭제, 정렬
producer_all1 = spark.sql('select citygas.*, oil.* from citygas left join oil on citygas.date = oil.oildate').drop('oildate').orderBy('date')


# 날짜 자르기
from pyspark.sql.functions import lit
producer_all1 = producer_all1.withColumn('year', col('date').substr(1, 4))\
.withColumn('month', col('date').substr(5, 2))\
.withColumn('relativePrice', lit(col('gasProducerPrice')/col('oilProducerPrice')))\
.drop('date')


producer2 = spark.read.json('/predict_gas/raw_data/producer_price/producer_price2.json')
producer2.show()

from pyspark.sql.functions import explode, col
producer_explode2 = producer2.select(explode(producer2['StatisticSearch']['row']))
producer_df2 = producer_explode2.select(producer_explode2.col['TIME'], producer_explode2.col['ITEM_NAME1'], producer_explode2.col['DATA_VALUE'])

producer_df2.createOrReplaceTempView('producer_df2')

# 원유 테이블
oil = producer_df2.where(col('col.ITEM_NAME1') == '원유정제처리제품')\
.withColumnRenamed('col.TIME', 'oildate')\
.withColumnRenamed('col.DATA_VALUE', 'oilProducerPrice').drop('col.ITEM_NAME1')

# 도시가스 테이블
citygas = producer_df2.where(col('col.ITEM_NAME1') == '도시가스')\
.dropDuplicates(['col.TIME']).withColumnRenamed('col.TIME', 'date')\
.withColumnRenamed('col.DATA_VALUE', 'gasProducerPrice').drop('col.ITEM_NAME1')

citygas.count() # 398
oil.count() # 397

citygas.createOrReplaceTempView('citygas')
oil.createOrReplaceTempView('oil')
# left join, 날짜 한 열 삭제, 정렬
producer_all2 = spark.sql('select citygas.*, oil.* from citygas left join oil on citygas.date = oil.oildate').drop('oildate').orderBy('date')


# 날짜 자르기
from pyspark.sql.functions import lit
producer_all2 = producer_all2.withColumn('year', col('date').substr(1, 4))\
.withColumn('month', col('date').substr(5, 2))\
.withColumn('relativePrice', lit(col('gasProducerPrice')/col('oilProducerPrice')))\
.drop('date')

producer_all1.createOrReplaceTempView('producer_all1')
producer_all2.createOrReplaceTempView('producer_all2')
producer_all = spark.sql('select * from producer_all1 union all select * from producer_all2')




producer_all.write.jdbc(url, dbtable, "append", properties={"driver": driver, "user": user, "password": password})



