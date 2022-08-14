import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import col, asc, regexp_replace, row_number
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkFiles


spark =SparkSession.builder.getOrCreate()

user="root"
password="1234"
url="jdbc:mysql://localhost:3306/phoenix"
driver="com.mysql.cj.jdbc.Driver"
dbtable="lngimport"

spark.sparkContext.addFile(f'hdfs:///predict_gas/raw_data/lng_import/lng_import.xls')

pd_df = pd.read_excel(SparkFiles.get('lng_import.xls'),'Sheet0')
pd_df.drop([0, 4, 5, 6], axis=0, inplace=True)
pd_df.drop([pd_df.columns[0], pd_df.columns[1]], axis=1, inplace=True)
pd_df=pd_df.transpose()


df_schema = StructType([
  StructField("year", StringType(), True),
  StructField("import", StringType(), True),
  StructField("demand", StringType(), True)
  ])
spark_df = spark.createDataFrame(pd_df, schema=df_schema)
spark_df = spark_df.select(col('year'), regexp_replace(col('import'), '\D', '').alias('import'), regexp_replace(col('demand'), '\D', '').alias('demand'))
spark_df = spark_df.select(col('year').cast('int'), col('import').cast('int'), col('demand').cast('int'))

window = Window.orderBy(col('year')).orderBy(asc(col('year')))
insert_df = spark_df.withColumn('lngImportId', row_number().over(window))

insert_df.write.jdbc(url, dbtable, "append", properties={"driver": driver, "user": user, "password": password})
