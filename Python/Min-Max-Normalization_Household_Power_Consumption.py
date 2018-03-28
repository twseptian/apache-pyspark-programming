#!/usr/bin/python
# Big data Mining and Applications HW#1
# Name          : Tri Wanda Septian
# Student ID    : 106998406
# Name          : Anggara Aji Saputra
# Student ID    : 106998412

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import IntegerType, DateType, DoubleType
# Using library of pyspark to anaysis minimum, maximum, mean and standar deviation
from pyspark.sql.functions import min,max,count,col

#Make connection to spark master
conf = SparkConf().setMaster("spark://sparklab1:7077").setAppName("Min-Max-Count_Household_Power_Consumption")
sc = SparkContext.getOrCreate(conf=conf)
sqlContext=SQLContext(sc)

#load dataset from hdfs 
raw_data = sc.textFile("hdfs://tri-virtual-machine:9000/user/tri/household_power_consumption.txt",use_unicode=False)

#remove & split ';' from datasets
df = raw_data.map(lambda x: x.split(";"))

#Create dataframe from dataset
df2 = sqlContext.createDataFrame(data = df.filter(lambda x:x[0]!='Date'),schema = df.filter(lambda x:x[0]=='Date').collect()[0])

#Change String format to DoubleType "Global_active_power","Global_reactive_power","Voltage","Global_intensity"
df3 = df2.withColumn("Global_active_power", df2["Global_active_power"].cast(DoubleType()))
df4 = df3.withColumn("Global_reactive_power", df3["Global_reactive_power"].cast(DoubleType()))
df5 = df4.withColumn("Voltage", df4["Voltage"].cast(DoubleType()))
df6 = df5.withColumn("Global_intensity", df5["Global_intensity"].cast(DoubleType()))

# Output the minimum, maximum normalization of the columns 'Global_active_power','Global_reactive_power','Voltage','Global_intentsity'
print "Minimum-Maximum Normalization Global_active_power"
(df6.select(min("Global_active_power").alias("MIN_Global_active_power"),max("Global_active_power").alias("MAX_Global_active_power")).\
            crossJoin(df6).withColumn("Min-Max_Normalization",(col("Global_active_power") - col("MIN_Global_active_power")) / (col("MAX_Global_active_power") - col("MIN_Global_active_power")))).\
            select("Global_active_power","MIN_Global_active_power","MAX_Global_active_power","Min-Max_Normalization").\
            coalesce(1).write.format("csv").options (header='true').save("min-max-normalization-global_active_power.csv") #save result to csv file in hdfs

print "Minimum-Maximum Normalization Global_reactive_power"
(df6.select(min("Global_reactive_power").alias("MIN_Global_Reactive_power"),max("Global_reactive_power").alias("MAX_Global_Reactive_power")).\
            crossJoin(df6).withColumn("Min-Max_Normalization",(col("Global_reactive_power") - col("MIN_Global_Reactive_power")) / (col("MAX_Global_Reactive_power") - col("MIN_Global_Reactive_power")))).\
            select("Global_Reactive_power","MIN_Global_Reactive_power","MAX_Global_Reactive_power","Min-Max_Normalization").\
            coalesce(1).write.format("csv").options (header='true').save("min-max-normalization-Global_reactive_power.csv") #save result to csv file in hdfs

print "Minimum-Maximum Normalization Voltage"
(df6.select(min("Voltage").alias("MIN_Voltage"),max("Voltage").alias("MAX_Voltage")).\
            crossJoin(df6).withColumn("Min-Max_Normalization",(col("Voltage") - col("MIN_Voltage")) / (col("MAX_Voltage") - col("MIN_Voltage")))).\
            select("Voltage","MIN_Voltage","MAX_Voltage","Min-Max_Normalization").\
            coalesce(1).write.format("csv").options (header='true').save("min-max-normalization-Voltage.csv") #save result to csv file in hdfs

print "Minimum-Maximum Normalization Global_intensity"
(df6.select(min("Global_intensity").alias("MIN_Global_Intensity"),max("Global_intensity").alias("MAX_Global_Intensity")).\
            crossJoin(df6).withColumn("Min-Max_Normalization",(col("Global_intensity") - col("MIN_Global_Intensity")) / (col("MAX_Global_Intensity") - col("MIN_Global_Intensity")))).select("Global_intensity","MIN_Global_Intensity","MAX_Global_Intensity","Min-Max_Normalization").\
            coalesce(1).write.format("csv").options (header='true').save("min-max-normalization-Global_Intensity.csv") #save result to csv file in hdfs
