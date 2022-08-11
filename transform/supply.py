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
#from pyspark.sql.window import *

spark =SparkSession.builder.getOrCreate()

user="root"
password="1234"
url="jdbc:mysql://localhost:3306/phoenix"
driver="com.mysql.cj.jdbc.Driver"
dbtable="supply" #table 이름 맞게 지정

#region_df = spark.read.format("jdbc").options(user=user, password=password, url=url, driver=driver, dbtable=dbtable).load()

for year in range(2001, 2021):
	for month in range(1,13):
		wb = xl.load_workbook(f'supply/supply{year}.xlsx')
		year_zf = str(year%2000).zfill(2)

		if year < 2004:
			ws = wb[f'{month}월']
		elif year < 2012:
			ws = wb[f'{year_zf}.{month}월']
		else:
			ws = wb[f'({year_zf}.{month}월)_부피']
		
		pd_df = pd.DataFrame(ws.values)
		pd_df.drop([0,1], inplace=True)
		
		df_schema = StructType([StructField(f"col{i}", StringType(), True) for i in range(len(pd_df.columns))])
		spark_df = spark.createDataFrame(pd_df, schema=df_schema)
		
		spark_df = spark_df.coalesce(1).select(monotonically_increasing_id().alias('id'), expr('*'))
		
		#용도별 공급량 부분만 짤라서 새로운 dataframe 만들기
		drop_col = []
		for i in spark_df.columns[3:]:
			if spark_df.where('id=0').select(col(i)).first()[0] == '용 도 별 공 급 량 (천㎥, 10,500kcal/㎥기준)' or spark_df.where('id=0').select(col(i)).first()[0] == '용 도 별 공 급 량 (천㎥, 10,400kcal/㎥기준)' or spark_df.where('id=0').select(col(i)).first()[0] == '용 도 별 공 급 량 (천㎥)':
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
		    if data != None:
		        stack.pop()
		        stack.append(data)
		    else:
		        pass
		    data_list.append(stack[0])
		
		data_col = [Row(id=str(idx), col0=data) for idx, data in enumerate(data_list)]
		data_col_df = spark.createDataFrame(data_col)
		
		spark_df = spark_df.join(data_col_df,spark_df.id == data_col_df.id, 'inner').drop(spark_df.col0).drop(data_col_df.id).sort(asc(col('id')))
		
		#지역별 회사들의 갯수를 세서, 기존의 table과 join
		spark_df.createOrReplaceTempView('df')
		count_df = spark.sql('select col0, count(*) as count from df group by col0')
		
		spark_df = spark_df.join(count_df, spark_df.col0 == count_df.col0, 'left').drop(count_df.col0).select('id', 'col0', 'col1','col2','count').sort(asc('id'))
		
		#지역별 회사들의 갯수가 한개인 지역들을 찾아서, col1열의 값을 “소  계”로 통일하기
		spark_df = spark_df.withColumn('col1', when(col('count') == '1' ,"소   계").otherwise(spark_df.col1))
		
		#col0가 지역이면서 col1이 “소  계”인 데이터로 테이블 생성
		region_list = ['서  울', '부  산','대  구','인  천','광  주','대  전','울  산','경  기','강  원','충  북','충  남','전  북','전  남','경  북','경  남','제  주','세  종']
		spark_df = spark_df.where(col('col1') == '소   계').where(col('col0').isin(region_list)).select('col0', col('col2').alias('합계'))
		
		#year , month 열 추가
		spark_df = spark_df.withColumn('year', lit(int('20'+ year_zf))).withColumn('month', lit(int(month)))
		
		#‘지역이름 → 설정한 지역 id로 바꾸기’ + ‘부피단위의 값 → 톤 단위의 값’
		region = Row('regionId', 'region')
		data = [region(idx+1, r) for idx, r in enumerate(region_list)]
		data_df = spark.createDataFrame(data)
		
		density = 0.7879
		
		spark_df = spark_df.join(data_df, spark_df.col0 == data_df.region, 'inner').sort(asc('regionId')).withColumn('합계',col('합계') * density).select(col('year').cast('int'),col('month').cast('int'),col('regionId').cast('int'),col('합계').alias('supply').cast('float'))
		
		#spark_df.show()
		#spark_df.printSchema()
		
		spark_df.write.jdbc(url, dbtable, "append", properties={"driver": driver, "user": user, "password": password})
