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
from pyspark.sql.functions import mean,stddev

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

# Output the mean and Standar deviation of the columns 'Global_active_power','Global_reactive_power','Voltage','Global_intentsity'
print "Show output Mean and Standar Deviation 'Global_active_power','Global_reactive_power','Voltage','Global_intensity'"
df6.select([mean("Global_active_power").alias("Mean Global Active Power"),stddev("Global_active_power").alias("Standar Deviation Global Active Power"),\
            mean("Global_reactive_power").alias("Mean Global Reactive Power"),stddev("Global_reactive_power").alias("Standar Deviation Global Reactive Power"),\
            mean("voltage").alias("Mean Voltage"),stddev("Voltage").alias("Standar Deviation Voltage"),\
            mean("Global_intensity").alias("Mean Global Intensity"),stddev("Global_intensity").alias("Standar Deviation Global Intensity")]).\
            coalesce(1).write.format("csv").options (header='true').save("mean-stddev_household_power_consumption.csv") #save result to csv file in hdfs