from pyspark.sql.functions import substring
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import monotonically_increasing_id , expr
from pyspark.sql.functions import regexp_replace
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession.builder.getOrCreate()

df = spark.read.format('csv').option('header','true').load('gas_cost.csv')

#region_id 설정
user="root"
password="1234"
url="jdbc:mysql://localhost:3306/phoenix"
driver="com.mysql.cj.jdbc.Driver"
dbtable="citygas_cost" #->table 이름 맞게 지정

region_df = spark.read.format("jdbc").options(user=user, password=password, url=url, driver=driver, dbtable="region").load()
#region_df.show()

# year/month DATE
df1 = df.select(substring(col('날짜'), 1, 4).alias('year'), substring(col('날짜'), 5, 2).alias('month'))
 
df1 = df1.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))

# 날짜 / 지역별
# 서울
df2 = df.select(df.columns[1], df.columns[2])
df2a = df2.select(col('서울').alias('citygasCost')).withColumn("regionId",lit('1'))
df2a = df2a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df2b = df2a.join(df1,df1['id'] == df2a['id'])
df2b = df2b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# 부산
df3 = df.select(df.columns[1], df.columns[3])
df3a = df3.select(col('부산').alias('citygasCost')).withColumn("regionId",lit('2'))
df3a = df3a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df3b = df3a.join(df1,df1['id'] == df3a['id'])
df3b = df3b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# 대구
df4 = df.select(df.columns[1], df.columns[4])
df4a = df4.select(col('대구').alias('citygasCost')).withColumn("regionId",lit('3'))
df4a = df4a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df4b = df4a.join(df1,df1['id'] == df4a['id'])
df4b = df4b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# 인천
df5 = df.select(df.columns[1], df.columns[5])
df5a = df5.select(col('인천').alias('citygasCost')).withColumn("regionId",lit('4'))
df5a = df5a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df5b = df5a.join(df1,df1['id'] == df5a['id'])
df5b = df5b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# 광주
df6 = df.select(df.columns[1], df.columns[6])
df6a = df6.select(col('광주').alias('citygasCost')).withColumn("regionId",lit('5'))
df6a = df6a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df6b = df6a.join(df1,df1['id'] == df6a['id'])
df6b = df6b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# 대전
df7 = df.select(df.columns[1], df.columns[7])
df7a = df7.select(col('대전').alias('citygasCost')).withColumn("regionId",lit('6'))
df7a = df7a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df7b = df7a.join(df1,df1['id'] == df7a['id'])
df7b = df7b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# 울산
df8 = df.select(df.columns[1], df.columns[8])
df8a = df8.select(col('울산').alias('citygasCost')).withColumn("regionId",lit('7'))
df8a = df8a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df8b = df8a.join(df1,df1['id'] == df8a['id'])
df8b = df8b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# 세종
df9 = df.select(df.columns[1], df.columns[9])
df9a = df9.select(col('세종').alias('citygasCost')).withColumn("regionId",lit('8'))
df9a = df9a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df9b = df9a.join(df1,df1['id'] == df9a['id'])
df9b = df9b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# 경기
df10 = df.select(df.columns[1], df.columns[10])
df10a = df10.select(col('경기').alias('citygasCost')).withColumn("regionId",lit('9'))
df10a = df10a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df10b = df10a.join(df1,df1['id'] == df10a['id'])
df10b = df10b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# 강원
df11 = df.select(df.columns[1], df.columns[11])
df11a = df11.select(col('강원').alias('citygasCost')).withColumn("regionId",lit('10'))
df11a = df11a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df11b = df11a.join(df1,df1['id'] == df11a['id'])
df11b = df11b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# 충북
df12 = df.select(df.columns[1], df.columns[12])
df12a = df12.select(col('충북').alias('citygasCost')).withColumn("regionId",lit('11'))
df12a = df12a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df12b = df12a.join(df1,df1['id'] == df12a['id'])
df12b = df12b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# 충남
df13 = df.select(df.columns[1], df.columns[13])
df13a = df13.select(col('충남').alias('citygasCost')).withColumn("regionId",lit('12'))
df13a = df13a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df13b = df13a.join(df1,df1['id'] == df13a['id'])
df13b = df13b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# 전북
df14 = df.select(df.columns[1], df.columns[14])
df14a = df14.select(col('전북').alias('citygasCost')).withColumn("regionId",lit('13'))
df14a = df14a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df14b = df14a.join(df1,df1['id'] == df14a['id'])
df14b = df14b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# 전남
df15 = df.select(df.columns[1], df.columns[15])
df15a = df15.select(col('전남').alias('citygasCost')).withColumn("regionId",lit('14'))
df15a = df15a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df15b = df15a.join(df1,df1['id'] == df15a['id'])
df15b = df15b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# 경북
df16 = df.select(df.columns[1], df.columns[16])
df16a = df16.select(col('경북').alias('citygasCost')).withColumn("regionId",lit('15'))
df16a = df16a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df16b = df16a.join(df1,df1['id'] == df16a['id'])
df16b = df16b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# 경남
df17 = df.select(df.columns[1], df.columns[17])
df17a = df17.select(col('경남').alias('citygasCost')).withColumn("regionId",lit('16'))
df17a = df17a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df17b = df17a.join(df1,df1['id'] == df17a['id'])
df17b = df17b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# 제주
df18 = df.select(df.columns[1], df.columns[18])
df18a = df18.select(col('제주').alias('citygasCost')).withColumn("regionId",lit('17'))
df18a = df18a.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
df18b = df18a.join(df1,df1['id'] == df18a['id'])
df18b = df18b.select(col('year'),col('month'), col('regionId'), col('citygasCost'))

# union (종합)

df_union = df2b.union(df3b).union(df4b).union(df5b).union(df6b).union(df7b).union(df8b).union(df9b).union(df10b).union(df11b).union(df12b).union(df13b).union(df14b).union(df15b).union(df16b).union(df17b).union(df18b)
df_union = df_union.select(col('year').cast('int'), col('month').cast('int'), col('regionId').cast('int'),regexp_replace(col('citygasCost'),',', '' ).alias('citygasCost').cast('int'))
df_union.show(df_union.count())

df_union.write.jdbc(url, dbtable, "append", properties={"driver": driver, "user": user, "password": password})
