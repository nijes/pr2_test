
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark =SparkSession.builder.getOrCreate()

user="root"
password="1234"
url="jdbc:mysql://localhost:3306/phoenix"
driver="com.mysql.cj.jdbc.Driver"
dbtable="heatindex"

df = spark.read.format('csv').option('header', 'true').option('encoding', 'cp949').load('/predict_gas/raw_data/heat/heat.csv')
df = df.select(col('날짜').substr(1, 4).alias('year'), col('날짜').substr(6, 2).alias('month'), '1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24')

df = df.groupBy('year','month').agg({'1':'sum','2':'sum','3':'sum','4':'sum','5':'sum','6':'sum','7':'sum','8':'sum','9':'sum','10':'sum','11':'sum','12':'sum','13':'sum','14':'sum','15':'sum','16':'sum','17':'sum','18':'sum','19':'sum','20':'sum','21':'sum','22':'sum','23':'sum','24':'sum'}).sort('year','month')

col_list = df.columns[2:]
df = df.withColumn('heatIndex',sum([col(c) for c in col_list])).select(col('year').cast('int'),col('month').cast('int'),col('heatIndex').cast('float')).sort('year','month')
df.write.jdbc(url, dbtable, "append", properties={"driver": driver, "user": user, "password": password})
