import pandas as pd
import openpyxl as xl
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id
#from pyspark.sql.functions import row_number
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from pyspark.sql.functions import asc
from pyspark.sql.functions import when
from pyspark.sql.functions import lit
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkFiles

#from pyspark.sql.window import *

spark =SparkSession.builder.getOrCreate()

user="root"
password="1234"
url="jdbc:mysql://localhost:3306/phoenix"
driver="com.mysql.cj.jdbc.Driver"
dbtable="household" #table 이름 맞게 지정

#####################파일(2001~2013)#####################
month = 12

for year in range(2001, 2014):
	  #wb = xl.load_workbook(f'supply/supply{year}.xlsx')
	  year_zf = str(year%2000).zfill(2)
	  spark.sparkContext.addFile(f'hdfs:///predict_gas/raw_data/supply/supply{year}.xlsx')

	  if year < 2004:
		  pd_df = pd.read_excel(SparkFiles.get(f'supply{year}.xlsx'),f'{month}월')
	  elif year < 2012:
		  pd_df = pd.read_excel(SparkFiles.get(f'supply{year}.xlsx'),f'{year_zf}.{month}월')
	  else:
		  pd_df = pd.read_excel(SparkFiles.get(f'supply{year}.xlsx'),f'({year_zf}.{month}월)_부피')
		
	  pd_df.drop(0, inplace=True)
	  df_schema = StructType([StructField(f"col{i}", StringType(), True) for i in range(len(pd_df.columns))])
	  spark_df = spark.createDataFrame(pd_df, schema=df_schema)
	
	  spark_df = spark_df.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
	
	#용도별 공급량 부분만 짤라서 새로운 dataframe 만들기
	  drop_col = []
	  for i in spark_df.columns[3:]:
		    if spark_df.where('id=0').select(col(i)).first()[0] == '용 도 별 수 요 가 수 (개)':
			      slice_col_name = i
			      break
		    drop_col.append(i)
	
	  spark_df = spark_df.drop(*(drop_col))
	
	#합계 부분의 열을 찾아서, 새로운 dataframe 만들기
	  for i in spark_df.columns[3:]:
		    if spark_df.where('id=1').select(col(i)).first()[0] == '합 계' or spark_df.where('id=1').select(col(i)).first()[0] == '합  계':
			      sum_col = i
			      break
	
	  spark_df = spark_df.select('id','col0','col1',col(sum_col).alias('col2'))
	
	#하나의 열을 빼내서, 처리한 후, 다시 집어넣기
	  target_row_list = spark_df.select('col0').collect()
	  target_data_list = [i[0] for i in target_row_list]
	
	  data_list = []
	  stack = ['initValue']
	  for data in target_data_list:
	      if data == 'NaN':
	          pass
	      else:
	          stack.pop()
	          stack.append(data)
	      data_list.append(stack[0])
	
	  data_col = [Row(id=str(idx), col0=data) for idx, data in enumerate(data_list)]
	  data_col_df = spark.createDataFrame(data_col)
	
	  spark_df = spark_df.join(data_col_df,spark_df.id == data_col_df.id, 'inner').drop(spark_df.col0).drop(data_col_df.id).sort(asc(col('id')))
	
	  spark_df.createOrReplaceTempView('df')
	  count_df = spark.sql('select col0, count(*) as count from df group by col0')
	
	  spark_df = spark_df.join(count_df, spark_df.col0 == count_df.col0, 'left').drop(count_df.col0).select('id', 'col0', 'col1','col2','count').sort(asc('id'))
	
	  spark_df = spark_df.withColumn('col1', when(col('count') == '1' ,"소   계").otherwise(spark_df.col1))
	
	  region_list = ['서  울', '부  산','대  구','인  천','광  주','대  전','울  산','경  기','강  원','충  북','충  남','전  북','전  남','경  북','경  남','제  주','세  종']
	  spark_df = spark_df.where(col('col1') == '소   계').where(col('col0').isin(region_list)).select('col0', col('col2').alias('합계'))
	
	  spark_df = spark_df.withColumn('year', lit(int('20'+ year_zf)))
	
	  region = Row('regionId', 'region')
	  data = [region(idx+1, r) for idx, r in enumerate(region_list)]
	  data_df = spark.createDataFrame(data)
	
	  spark_df = spark_df.join(data_df, spark_df.col0 == data_df.region, 'inner').sort(asc('regionId')).withColumn('합계',col('합계')).select(col('year').cast('int'), col('regionId').cast('int'), col('합계').alias('household').cast('int'))
	  #spark_df.show(spark_df.count())
	  #spark_df.printSchema()
	  spark_df.write.jdbc(url, dbtable, "append", properties={"driver": driver, "user": user, "password": password})

#####################파일(2001~2013)#####################
#df = spark.read.csv("/user/folder/household/household.csv", header='true')
df = spark.read.csv("/predict_gas/raw_data/household/household.csv", header='true')
#df.show()


#columns_list = df.columns
#col_list = []
#for col in columns_list[1:20]:
#   col_list.append(col)


#df2 = df.select(*(col_list))
#household_drop = df.drop(col("('전국', '합계')"))
#household_drop.show()

#df3 = df2.select(col("날짜"),col("('전국', '합계')").alias("전국"),col("('서울', '합계')").alias("서울"),col("('부산', '합계')").alias("부산"),col("('대구', '합계')").alias("대구"),col("('인천', '합계')").alias("인천"),col("('광주', '합계')").alias("광주"),col("('대전', '합계')").alias("대전"),col("('울산', '합계')").alias("울산"),col("('세종', '합계')").alias("세종"),col("('경기', '합계')").alias("경기"),col("('강원', '합계')").alias("강원"),col("('충북', '합계')").alias("충북"),col("('전북', '합계')").alias("전북"),col("('전남', '합계')").alias("전남"),col("('경북', '합계')").alias("경북"),col("('경남', '합계')").alias("경남"),col("('제주', '합계')").alias("제주")
#df3 = df2.select(col("날짜"),col("('전국', '합계')").alias("전국"),col("('서울', '합계')").alias("서울"),col("('부산', '합계')").alias("부산"),col("('대구', '합계')").alias("대구"),col("('인천', '합계')").alias("인천"),col("('광주', '합계')").alias("광주"),col("('대전', '합계')").alias("대전"),col("('울산', '합계')").alias("울산"),col("('세종', '합계')").alias("세종"),col("('경기', '합계')").alias("경기"),col("('강원', '합계')").alias("강원"),col("('충북', '합계')").alias("충북"),col("('전북', '합계')").alias("전북"),col("('전남', '합계')").alias("전남"),col("('경북', '합계')").alias("경북"),col("('경남', '합계')").alias("경남"),col("('제주', '합계')").alias("제주")
household = df.select(col('날짜').alias("date"),col("('서울', '합계')").alias("서울"),col("('부산', '합계')").alias("부산"),col("('대구', '합계')").alias("대구"),col("('인천', '합계')").alias("인천"),col("('광주', '합계')").alias("광주"),col("('대전', '합계')").alias("대전"),col("('울산', '합계')").alias("울산"),col("('세종', '합계')").alias("세종"),col("('경기', '합계')").alias("경기"),col("('강원', '합계')").alias("강원"),col("('충북', '합계')").alias("충북"),col("('충남', '합계')").alias("충남"),col("('전북', '합계')").alias("전북"),col("('전남', '합계')").alias("전남"),col("('경북', '합계')").alias("경북"),col("('경남', '합계')").alias("경남"),col("('제주', '합계')").alias("제주"))
household.createOrReplaceTempView('household')


df_서울 = spark.sql("select date, `서울` from household")
df_부산 = spark.sql("select date, `부산` from household")
df_대구 = spark.sql("select date, `대구` from household")
df_인천 = spark.sql("select date, `인천` from household")
df_광주 = spark.sql("select date, `광주` from household")
df_대전 = spark.sql("select date, `대전` from household")
df_울산 = spark.sql("select date, `울산` from household")
df_세종 = spark.sql("select date, `세종` from household")
df_경기 = spark.sql("select date, `경기` from household")
df_강원 = spark.sql("select date, `강원` from household")
df_충북 = spark.sql("select date, `충북` from household")
df_충남 = spark.sql("select date, `충남` from household")
df_전북 = spark.sql("select date, `전북` from household")
df_전남 = spark.sql("select date, `전남` from household")
df_경북 = spark.sql("select date, `경북` from household")
df_경남 = spark.sql("select date, `경남` from household")
df_제주 = spark.sql("select date, `제주` from household")
#df_서울.show()

from pyspark.sql.functions import lit
df_서울 = df_서울.withColumn("regionName",lit("서울"))
df_부산 = df_부산.withColumn("regionName",lit("부산"))
df_대구 = df_대구.withColumn("regionName",lit("대구"))
df_인천 = df_인천.withColumn("regionName",lit("인천"))
df_광주 = df_광주.withColumn("regionName",lit("광주"))
df_대전 = df_대전.withColumn("regionName",lit("대전"))
df_울산 = df_울산.withColumn("regionName",lit("울산"))
df_세종 = df_세종.withColumn("regionName",lit("세종"))
df_경기 = df_경기.withColumn("regionName",lit("경기"))
df_강원 = df_강원.withColumn("regionName",lit("강원"))
df_충북 = df_충북.withColumn("regionName",lit("충북"))
df_충남 = df_충남.withColumn("regionName",lit("충남"))
df_전북 = df_전북.withColumn("regionName",lit("전북"))
df_전남 = df_전남.withColumn("regionName",lit("전남"))
df_경북 = df_경북.withColumn("regionName",lit("경북"))
df_경남 = df_경남.withColumn("regionName",lit("경남"))
df_제주 = df_제주.withColumn("regionName",lit("제주"))

df_서울.createOrReplaceTempView('`df_서울`') 
df_부산.createOrReplaceTempView('`df_부산`') 
df_대구.createOrReplaceTempView('`df_대구`')
df_인천.createOrReplaceTempView('`df_인천`') 
df_광주.createOrReplaceTempView('`df_광주`') 
df_대전.createOrReplaceTempView('`df_대전`') 
df_울산.createOrReplaceTempView('`df_울산`') 
df_세종.createOrReplaceTempView('`df_세종`') 
df_경기.createOrReplaceTempView('`df_경기`') 
df_강원.createOrReplaceTempView('`df_강원`') 
df_충북.createOrReplaceTempView('`df_충북`') 
df_충남.createOrReplaceTempView('`df_충남`') 
df_전북.createOrReplaceTempView('`df_전북`') 
df_전남.createOrReplaceTempView('`df_전남`') 
df_경북.createOrReplaceTempView('`df_경북`') 
df_경남.createOrReplaceTempView('`df_경남`') 
df_제주.createOrReplaceTempView('`df_제주`')

from pyspark.sql.functions import asc, regexp_replace
household_all = spark.sql("select * from `df_서울` union select * from `df_부산` union select * from `df_대구` union select * from `df_인천` union select * from `df_광주` union select * from `df_대전` union select * from `df_울산` union select * from `df_세종` union select * from `df_경기` union select * from `df_강원` union select * from `df_충북` union select * from `df_충남` union select * from `df_전북` union select * from `df_전남` union select * from `df_경북` union select * from `df_경남` union select * from `df_제주` ")\
.withColumnRenamed("서울", "household")\
.withColumnRenamed("date", "year")\
.orderBy(asc(col('year')))\
.withColumn('household', regexp_replace('household', ',', ''))

#user="root"
#password="1234"
#url="jdbc:mysql://localhost:3306/phoenix"
#driver="com.mysql.cj.jdbc.Driver"
region_df = spark.read.format("jdbc").options(user=user, password=password, url=url, driver=driver, dbtable="region").load()
region_df.withColumnRenamed('regionName','regionName_drop')

region_df.createOrReplaceTempView('region_df')
household_all.createOrReplaceTempView('household_all')
household_insert = spark.sql('''
select household_all.*, region_df.* 
from household_all
join region_df 
on household_all.regionName = region_df.regionName
''')
household_insert.createOrReplaceTempView('household_insert')
household_insert = household_insert.select(col('year').cast('int'), col('regionId'), col('household')).orderBy(asc(col('year')))

household_insert.write.jdbc(url, dbtable, "append", properties={"driver": driver, "user": user, "password": password})
