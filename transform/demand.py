from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col, lit, asc
from pyspark.sql.types import IntegerType

sc = SparkContext()
spark =SparkSession.builder.getOrCreate()

user="root"
password="1234"
url="jdbc:mysql://localhost:3306/phoenix"
driver="com.mysql.cj.jdbc.Driver"
dbtable="demand"

# df
demand_df = spark.read.csv('/predict_gas/raw_data/demand/demand.csv', header='true', encoding='cp949')

# sql table
demand_df.createOrReplaceTempView('demand_sql')

# date table(sql)
df_date = spark.sql('select `연`, `월` from demand_sql')
from pyspark.sql.functions import monotonically_increasing_id
df_date_id = df_date.withColumn("date_id", monotonically_increasing_id())
# df_date_id.show(df_date_id.count())

# quantity table(sql)
df_quantity = spark.sql('''
select `강원`*0.7879 `강원`, `서울`*0.7879 `서울`, `경기`*0.7879 `경기`, `인천`*0.7879 `인천`, 
`경남`*0.7879 `경남`, `경북`*0.7879 `경북`, `광주`*0.7879 `광주`, `대구`*0.7879 `대구`, 
`대전`*0.7879 `대전`, `부산`*0.7879 `부산`,`세종`*0.7879 `세종`,`울산`*0.7879 `울산`,
`전남`*0.7879 `전남`, `전북`*0.7879 `전북`, `제주`*0.7879 `제주`, `충남`*0.7879 `충남`, `충북`*0.7879 `충북`
from demand_sql
''')

df_quantity_id = df_quantity.withColumn("quantity_id", monotonically_increasing_id())
df_quantity_id.show(df_quantity_id.count())

# join
df_date_id.createOrReplaceTempView('df_date_id')
df_quantity_id.createOrReplaceTempView('df_quantity_id')
demand = spark.sql("""
select `연`, `월`, df_quantity_id.*
from df_date_id
join df_quantity_id
on df_date_id.date_id = df_quantity_id.quantity_id
""")
demand.show()







# 지역 별로 테이블을 만들어서 union 해버리면 끝!
demand.createOrReplaceTempView('demand')
# df_강원.createOrReplaceTempView('df_강원')
df_강원 = spark.sql('''
select `연`, `월`, `강원` as demand, quantity_id
from demand
'''
)
# withColumn
df_강원 = df_강원.withColumn("regionName", lit("강원"))

df_서울 = spark.sql('''
select `연`, `월`, `서울` as demand, quantity_id
from demand
'''
)
df_서울 = df_서울.withColumn("regionName", lit("서울"))

df_경기 = spark.sql('''
select `연`, `월`, `경기` as demand, quantity_id
from demand
'''
)
df_경기 = df_경기.withColumn("regionName", lit("경기"))

df_인천 = spark.sql('''
select `연`, `월`, `인천` as demand, quantity_id
from demand
'''
)
df_인천 = df_인천.withColumn("regionName", lit("인천"))

df_경남 = spark.sql('''
select `연`, `월`, `경남` as demand, quantity_id
from demand
'''
)
df_경남 = df_경남.withColumn("regionName", lit("경남"))

df_경북 = spark.sql('''
select `연`, `월`, `경북` as demand, quantity_id
from demand
'''
)
df_경북 = df_경북.withColumn("regionName", lit("경북"))

df_광주 = spark.sql('''
select `연`, `월`, `광주` as demand, quantity_id
from demand
'''
)
df_광주 = df_광주.withColumn("regionName", lit("광주"))

df_대구 = spark.sql('''
select `연`, `월`, `대구` as demand, quantity_id
from demand
'''
)
df_대구 = df_대구.withColumn("regionName", lit("대구"))

df_대전 = spark.sql('''
select `연`, `월`, `대전` as demand, quantity_id
from demand
'''
)
df_대전 = df_대전.withColumn("regionName", lit("대전"))

df_부산 = spark.sql('''
select `연`, `월`, `부산` as demand, quantity_id
from demand
'''
)
df_부산 = df_부산.withColumn("regionName", lit("부산"))

df_세종 = spark.sql('''
select `연`, `월`, `세종` as demand, quantity_id
from demand
'''
)
df_세종 = df_세종.withColumn("regionName", lit("세종"))

df_울산 = spark.sql('''
select `연`, `월`, `울산` as demand, quantity_id
from demand
'''
)
df_울산 = df_울산.withColumn("regionName", lit("울산"))

df_전남 = spark.sql('''
select `연`, `월`, `전남` as demand, quantity_id
from demand
'''
)
df_전남 = df_전남.withColumn("regionName", lit("전남"))

df_전북 = spark.sql('''
select `연`, `월`, `전북` as demand, quantity_id
from demand
'''
)
df_전북 = df_전북.withColumn("regionName", lit("전북"))

df_제주 = spark.sql('''
select `연`, `월`, `제주` as demand, quantity_id
from demand
'''
)
df_제주 = df_제주.withColumn("regionName", lit("제주"))

df_충남 = spark.sql('''
select `연`, `월`, `충남` as demand, quantity_id
from demand
'''
)
df_충남 = df_충남.withColumn("regionName", lit("충남"))

df_충북 = spark.sql('''
select `연`, `월`, `충북` as demand, quantity_id
from demand
'''
)
df_충북 = df_충북.withColumn("regionName", lit("충북"))

'''
+----+---+------------------+-----------+----------+
|  연| 월|              충북|quantity_id|regionName|
+----+---+------------------+-----------+----------+
|1989|  1|               0.0|          0|      충북|
|1989|  2|               0.0|          1|      충북|
|1989|  3|               0.0|          2|      충북|
|1989|  4|               0.0|          3|      충북|
|1989|  5|               0.0|          4|      충북|
|1989|  6|               0.0|          5|      충북|
|1989|  7|               0.0|          6|      충북|
|1989|  8| 7.091100000000001|          7|      충북|
|1989|  9|           16.5459|          8|      충북|
|1989| 10|55.940900000000006|          9|      충북|
|1989| 11|          137.0946|         10|      충북|
|1989| 12|334.06960000000004|         11|      충북|
|1990|  1|          568.0759|         12|      충북|
|1990|  2|448.31510000000003|         13|      충북|
|1990|  3|404.98060000000004|         14|      충북|
|1990|  4|          309.6447|         15|      충북|
|1990|  5|          186.7323|         16|      충북|
|1990|  6|120.54870000000001|         17|      충북|
|1990|  7|          114.2455|         18|      충북|
|1990|  8| 98.48750000000001|         19|      충북|
+----+---+------------------+-----------+----------+
only showing top 20 rows
'''


# tempview 생성
# df_강원.createOrReplaceTempView('df_강원') <- 안 됨
df_강원.createOrReplaceTempView('`df_강원`')
df_서울.createOrReplaceTempView('`df_서울`')
df_경기.createOrReplaceTempView('`df_경기`')
df_인천.createOrReplaceTempView('`df_인천`')
df_경남.createOrReplaceTempView('`df_경남`')
df_경북.createOrReplaceTempView('`df_경북`')
df_광주.createOrReplaceTempView('`df_광주`')
df_대구.createOrReplaceTempView('`df_대구`')
df_대전.createOrReplaceTempView('`df_대전`')
df_부산.createOrReplaceTempView('`df_부산`')
df_세종.createOrReplaceTempView('`df_세종`')
df_울산.createOrReplaceTempView('`df_울산`')
df_전남.createOrReplaceTempView('`df_전남`')
df_전북.createOrReplaceTempView('`df_전북`')
df_제주.createOrReplaceTempView('`df_제주`')
df_충남.createOrReplaceTempView('`df_충남`')
df_충북.createOrReplaceTempView('`df_충북`')



# Union

demand_all = spark.sql('''
select *
from `df_강원`
union
select *
from `df_서울`
union
select *
from `df_경기`
union
select *
from `df_인천`
union
select *
from `df_경남`
union
select *
from `df_경북`
union
select *
from `df_광주`
union
select *
from `df_대구`
union
select *
from `df_대전`
union
select *
from `df_부산`
union
select *
from `df_세종`
union
select *
from `df_울산`
union
select *
from `df_전남`
union
select *
from `df_전북`
union
select *
from `df_제주`
union
select *
from `df_충남`
union
select *
from `df_충북`
'''
)






# 열 이름 전환
demand_all.createOrReplaceTempView('demand_all')
demand_all = spark.sql(
'''
select `연` as year, `월` as month, demand, quantity_id, regionName
from demand_all
'''
)

# 데이터 타입 변경 (string -> integer)
demand_final = demand_all.select(demand_all.year.cast(IntegerType()), demand_all.month.cast(IntegerType()),
demand_all.regionName, demand_all.demand).orderBy(asc(col('year')), asc(col('month')))


# 지역
region_df = spark.read.format("jdbc").options(user=user, password=password, url=url, driver=driver, dbtable="region").load()
region_df.show()

# demand
demand_final.createOrReplaceTempView('demand_final')
region_df.createOrReplaceTempView('region_df')
demand_test = spark.sql('''
select demand_final.*, region_df.regionId
from demand_final
join region_df
on demand_final.regionName = region_df.regionName
order by year asc, month asc
'''
)
demand_insert = demand_test.drop(col('regionName'))
demand_insert.show()


demand_insert.write.jdbc(url, dbtable, "append", properties={"driver": driver, "user": user, "password": password})


