from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, asc, when, regexp_replace, row_number
from pyspark.sql.types import *
from pyspark.sql.window import Window


sc = SparkContext()
spark =SparkSession.builder.getOrCreate()

user="root"
password="1234"
url="jdbc:mysql://localhost:3306/phoenix"
driver="com.mysql.cj.jdbc.Driver"
dbtable="avg_temp" #table 이름 맞게 지정


######################################################

# csv파일 불러오기
df = spark.read.format('csv').option('header', 'true').load('temp.csv')

# 일시를 연도, 월을 나눈 후 컬럼 이름 재설정
df1 = df.select(substring(col('일시'), 1, 4).alias('year')\
, substring(col('일시'), 6, 2).alias('month'), col('지점명').alias('region')\
, col('평균기온(℃)').alias('avgTemp'))

# 시군구 기준으로 region 설정된 값들 시도 기준으로 변경
df_region = df1.withColumn('region', 
when(df1.region.endswith('수원'),regexp_replace(df1.region,'수원','경기')) 
     .when(df1.region.endswith('강릉'),regexp_replace(df1.region,'강릉','강원')) 
     .when(df1.region.endswith('충주'),regexp_replace(df1.region,'충주','충북'))
     .when(df1.region.endswith('천안'),regexp_replace(df1.region,'천안','충남'))
     .when(df1.region.endswith('전주'),regexp_replace(df1.region,'전주','전북'))
     .when(df1.region.endswith('고흥'),regexp_replace(df1.region,'고흥','전남'))
     .when(df1.region.endswith('청송군'),regexp_replace(df1.region,'청송군','경북'))
     .when(df1.region.endswith('김해시'),regexp_replace(df1.region,'김해시','경남'))
     .otherwise(df1.region)
)

# mysql db에 저장되어 있는 region 테이블 불러오기
region_df = spark.read.format("jdbc").options(user=user, password=password, url=url, driver=driver, dbtable="region").load()


# region 테이블과 조인하여 regionId 컬럼 추가
df_join = df_region.join(region_df, df_region.region==region_df.regionName, 'left_outer')

# 최종적으로 저장될 데이터 정리
insert_df0 = df_join.select( df_join.year.cast(IntegerType()), df_join.month.cast(IntegerType())\
, col('regionId'), col('avgTemp')).drop('region', 'regionName')\
.orderBy(asc(col('year')), asc(col('month')), asc(col('regionId')))

# 1씩 증가하는 아이디컬럼 추가
window = Window.orderBy(col('year')).orderBy(asc(col('year')), asc(col('month')), asc(col('regionId')))
insert_df = insert_df0.withColumn('avgtempId', row_number().over(window))

#######################################################

insert_df.write.jdbc(url, dbtable, "append", properties={"driver": driver, "user": user, "password": password})

