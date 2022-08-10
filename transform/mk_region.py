from pyspark import SparkContext
from pyspark.sql import SparkSession


sc = SparkContext()
spark =SparkSession.builder.getOrCreate()


user="root"
password="1234"
url="jdbc:mysql://localhost:3306/phoenix"
driver="com.mysql.cj.jdbc.Driver"
dbtable="region"


#region_df = spark.read.format("jdbc").options(user=user, password=password, url=url, driver=driver, dbtable=dbtable).load()
#region_df.show()


######################################################

region_insert = [
(1, '서울'),
(2, '부산'),
(3, '대구'),
(4, '인천'),
(5, '광주'),
(6, '대전'),
(7, '울산'),
(8, '경기'),
(9, '강원'),
(10, '충북'),
(11, '충남'),
(12, '전북'),
(13, '전남'),
(14, '경북'),
(15, '경남'),
(16, '제주'),
(17, '세종')
]

#######################################################

insert_df = sc.parallelize(region_insert).toDF(['regionId', 'regionName'])
#insert_df.show()

insert_df.write.jdbc(url, dbtable, "append", properties={"driver": driver, "user": user, "password": password})
#region_df.show()
