#required imports for running the spark program
import pyspark
from pyspark import SparkContext,SparkConf
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql import HiveContext
import datetime

#create spark context
#conf = SparkConf().set("spark.sql.warehouse.dir","/apps/hive/warehouse/")
#sc=SparkContext(conf=conf)
sqlContext=SQLContext(sc)

#creating hive context to fetch data from hive tables 
hive_context = HiveContext(sc)

#loading the data from hive tables into spark dataframes 
df_retail_sales =  hive_context.table("retail_project.retail_sales")
df_retail_stores = hive_context.table("retail_project.retail_stores")
df_retail_features = hive_context.table("retail_project.retail_features")

#converting data frame to temporary tables 
df_retail_sales.createOrReplaceTempView("spark_tab_retail_sales")
df_retail_stores.createOrReplaceTempView("spark_tab_retail_stores")
df_retail_features.createOrReplaceTempView("spark_tab_retail_features")

#Query-01
#the department-wide sales for each store

query_01 = open("/home/reallegendscorp9155/pyspark_project_retail/query_files/query_01.txt")
query01 = query_01.read()
df_q1 = sqlContext.sql(query01) 
df_q1.createOrReplaceTempView("temp_tab01")


#Query-02
#Fuel price in stores which are in place with average temperature between 60 and 80 degrees 

query_02 = open("/home/reallegendscorp9155/pyspark_project_retail/query_files/query_02.txt")
query02 = query_02.read()
df_q2 = sqlContext.sql(query02)
df_q2.createOrReplaceTempView("temp_tab02")


#Query-03
#for stores having department 65 and 85 and whose weekly sales is greater than 30K and whose CPI is less than 200 , find the sum of markdown1

query_03 = open("/home/reallegendscorp9155/pyspark_project_retail/query_files/query_03.txt")
query03 = query_03.read()
df_q3 = sqlContext.sql(query03)
df_q3.createOrReplaceTempView("temp_tab03")


#transformed data is loaded in hive tables 
hive_context.sql("DROP TABLE IF EXISTS retail_project.htab01")
hive_context.sql("create table retail_project.htab01  as select * from temp_tab01");
hive_context.sql("DROP TABLE IF EXISTS retail_project.htab02")
hive_context.sql("create table retail_project.htab02  as select * from temp_tab02");
hive_context.sql("DROP TABLE IF EXISTS retail_project.htab03")
hive_context.sql("create table retail_project.htab03  as select * from temp_tab03");

