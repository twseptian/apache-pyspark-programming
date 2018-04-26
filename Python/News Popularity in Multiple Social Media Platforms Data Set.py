#!/usr/bin/python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType, DateType, IntegerType


spark = SparkSession.builder.appName("HW#2").getOrCreate()

df_news = spark.read.csv("/home/twster/Spark/Projects/datasets/hw2/News_Final.csv", header=True, inferSchema=True)

#change types of PublishDate to TimestampType
df2_news = df_news.withColumn("PublishDate", unix_timestamp("PublishDate", "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))
#check again the Schema of Dataset
df2_news.select("PublishDate").printSchema()

import pyspark.sql.functions as F

#create new column from splited PublishDate -> Date and Time
split_PublishDate = F.split(df2_news['PublishDate'], ' ')
df3_news = df2_news.withColumn('Date', split_PublishDate.getItem(0)).withColumn('Time', split_PublishDate.getItem(1))

#Change Date column to DateType
df4_news = df3_news.withColumn('Date', df3_news['Date'].cast(DateType()))

#Title
TitleWords = df4_news.withColumn("Title", explode(split("Title", " ")))

TitleWords.groupBy("Title").count().orderBy(desc("count"))\
            .withColumnRenamed("Title","Words in Title")\
            .coalesce(1).write.format("csv").options (header='true')\
            .save("output/Q1/"+"Wordcount_Title_in_Total")
            
TitleWords.groupBy("Title","Topic").count().orderBy(desc("count"))\
            .withColumnRenamed("Title","Words in Title")\
            .coalesce(1).write.format("csv").options (header='true')\
            .save("output/Q1/"+"Wordcount_Title_per_Topic")
            
TitleWords.groupBy("Title","PublishDate").count()\
            .withColumnRenamed("Title","Word in Title")\
            .coalesce(1).write.format("csv").options (header='true')\
            .save("output/Q1/"+"Wordcount_Title_PublishDate")
            
TitleWords.groupBy("Title","Date").count().withColumnRenamed("Title","Words in Title")\
            .coalesce(1).write.format("csv").options(header='true')\
            .save("output/Q1/"+"Wordcount_Title_per_Day")

TitleWords.groupBy("Title","Time").count().withColumnRenamed("Title","Words in Title")\
            .coalesce(1).write.format("csv").options(header='true')\
            .save("output/Q1/"+"Wordcount_Title_per_Time")

#Headline
HeadlineWords = df4_news.withColumn("Headline", explode(split("Headline", " ")))
HeadlineWords.groupBy("Headline").count().orderBy(desc("count"))\
                .coalesce(1).write.format("csv").options(header='true')\
                .save("output/Q1/"+"Wordcount_Headline")

HeadlineWords.groupBy("Headline","Topic").count().orderBy(desc("count"))\
                .withColumnRenamed("Headline","Words in Headline")\
                .coalesce(1).write.format("csv").options(header='true')\
                .save("output/Q1/"+"Wordcount_Headline_per_Topic")

HeadlineWords.groupBy("Headline","PublishDate").count()\
                .coalesce(1).write.format("csv").options(header='true')\
                .save("output/Q1/"+"Wordcount_Headline_per_PublishDate")

HeadlineWords.groupBy("Headline","Date").count()\
                .coalesce(1).write.format("csv").options(header='true')\
                .save("output/Q1/"+"Wordcount_Headline_per_Date")
        
HeadlineWords.groupBy("Headline","Time").count()\
                .coalesce(1).write.format("csv").options(header='true')\
                .save("output/Q1/"+"Wordcount_headline_per_Time")

# (2) In social feedback data, calculate the average popularity of each news by hour, and by day, respectively (for each platform)
#in this case,we are using rdd
#list files from dataset
fileFBList=['Facebook_Economy', 'Facebook_Microsoft', 'Facebook_Obama', 'Facebook_Palestine']
fileGPlusList=['GooglePlus_Economy', 'GooglePlus_Microsoft', 'GooglePlus_Obama', 'GooglePlus_Palestine']
fileLinkedInList=['LinkedIn_Economy', 'LinkedIn_Microsoft', 'LinkedIn_Obama', 'LinkedIn_Palestine']

header_per_hour=['IDLink'] + ['TS'+str((count+1)*3) for count in range(48)]
header_per_day=['IDLink'] + ['TS'+str((count+1)*72) for count in range(2)]


# #### Feedback From Facebook
from operator import add
#Social media Facebook
SocMedia = 'Facebook'

Facebook_per_Hour = open('output/Q2/'+'Facebook_per_Hour.csv','w')
Facebook_per_Day = open('output/Q2/'+'Facebook_per_Day.csv','w')

for FileFB in fileFBList:
    facebook_feedback = spark.read.csv('/home/twster/Spark/Projects/datasets/hw2/'+FileFB+'.csv'
                                       ,header=True,inferSchema=True)
    popularity_avg_perhour = facebook_feedback.select(header_per_hour)\
                                                .rdd.map(list).flatMap(lambda x:((x[0], element) for element in x[1:]))\
                                                .reduceByKey(add).map(lambda x:(x[0], x[1]/48)).sortByKey()\
                                                .map(lambda x:('ID'+str(x[0]), x[1])).collect()
    popularity_avg_perday = facebook_feedback.select(header_per_day)\
                                                .rdd.map(list).flatMap(lambda x:((x[0], element) for element in x[1:]))\
                                                .reduceByKey(add).map(lambda x:(x[0], x[1]/2)).sortByKey()\
                                                .map(lambda x:('ID'+str(x[0]), x[1])).collect()
    
    if FileFB.split('_')[0] == SocMedia:
        Facebook_per_hour = open('output/Q2/'+'Facebook_per_Hour.csv','w')
        Facebook_per_day = open('output/Q2/'+'Facebook_per_Day.csv','w')
        for j in popularity_avg_perhour:
            k = ':'.join([str(l)for l in j])
            Facebook_per_hour.write(k + '\n')
        for j in popularity_avg_perday:
            k = ':'.join([str(l)for l in j])
            Facebook_per_day.write(k + '\n')
        Facebook_per_hour.close()
        Facebook_per_day.close()


# #### Feedback From GooglePlus

#Social media GooglePlus
SocMedia = 'GooglePlus'

GooglePlus_per_Hour = open('output/Q2/'+'GooglePlus_per_Hour.csv','w')
GooglePlus_per_Day = open('output/Q2/'+'GooglePlus_per_Day.csv','w')

for FileGPlus in fileGPlusList:
    gplus_feedback = spark.read.csv('/home/twster/Spark/Projects/datasets/hw2/'+FileGPlus+'.csv'
                                    ,header=True,inferSchema=True)
    popularity_avg_perhour = gplus_feedback.select(header_per_hour)\
                                            .rdd.map(list).flatMap(lambda x:((x[0], element) for element in x[1:]))\
                                            .reduceByKey(add).map(lambda x:(x[0], x[1]/48)).sortByKey()\
                                            .map(lambda x:('ID'+str(x[0]), x[1])).collect()
    popularity_avg_perday = gplus_feedback.select(header_per_day).rdd.map(list)\
                                            .flatMap(lambda x:((x[0], element) for element in x[1:]))\
                                            .reduceByKey(add).map(lambda x:(x[0], x[1]/2)).sortByKey()\
                                            .map(lambda x:('ID'+str(x[0]), x[1])).collect()
    
    if FileGPlus.split('_')[0] == SocMedia:
        GooglePlus_per_hour = open('output/Q2/'+'GooglePlus_per_Hour.csv','w')
        GooglePlus_per_day = open('output/Q2/'+'GooglePlus_per_Day.csv','w')
        for j in popularity_avg_perhour:
            k = ':'.join([str(l)for l in j])
            GooglePlus_per_hour.write(k + '\n')
        for j in popularity_avg_perday:
            k = ':'.join([str(l)for l in j])
            GooglePlus_per_day.write(k + '\n')
        GooglePlus_per_hour.close()
        GooglePlus_per_day.close()


# #### Feedback From LinkedIn

#Social media LinkedIn
SocMedia = 'LinkedIn'

LinkedIn_per_Hour = open('output/Q2/'+'LinkedIn_per_Hour.csv','w')
LinkedIn_per_Day = open('output/Q2/'+'LinkedIn_per_Day.csv','w')

for FileLinkedIn in fileLinkedInList:
    linkedin_feedback = spark.read.csv('/home/twster/Spark/Projects/datasets/hw2/'+FileLinkedIn+'.csv'
                                       ,header=True,inferSchema=True)
    popularity_avg_perhour = linkedin_feedback.select(header_per_hour)\
                                                .rdd.map(list).flatMap(lambda x:((x[0], element) for element in x[1:]))\
                                                .reduceByKey(add).map(lambda x:(x[0], x[1]/48)).sortByKey()\
                                                .map(lambda x:('ID'+str(x[0]), x[1])).collect()
    popularity_avg_perday = linkedin_feedback.select(header_per_day)\
                                                .rdd.map(list).flatMap(lambda x:((x[0], element) for element in x[1:]))\
                                                .reduceByKey(add).map(lambda x:(x[0], x[1]/2)).sortByKey()\
                                                .map(lambda x:('ID'+str(x[0]), x[1])).collect()
    
    if FileLinkedIn.split('_')[0] == SocMedia:
        LinkedIn_per_hour = open('output/Q2/'+'LinkedIn_per_Hour.csv','w')
        LinkedIn_per_day = open('output/Q2/'+'LinkedIn_per_Day.csv','w')
        for j in popularity_avg_perhour:
            k = ':'.join([str(l)for l in j])
            LinkedIn_per_hour.write(k + '\n')
        for j in popularity_avg_perday:
            k = ':'.join([str(l)for l in j])
            LinkedIn_per_day.write(k + '\n')
        LinkedIn_per_hour.close()
        LinkedIn_per_day.close()


# ### (3) In news data, calculate the sum and average sentiment score of each topic, respectively
# #### Summary SentimentTitle
df_SentimentScore = df4_news.withColumn("SentimentScore", df4_news.SentimentTitle+df4_news.SentimentHeadline)

df_SentimentScore.filter(df_SentimentScore['Topic'] == 'obama').groupBy('Topic').agg(F.sum("SentimentScore").alias("Summary"))\
                    .coalesce(1).write.format("csv").options(header='true').save("output/Q3/"+"SentimentScore_Sum_Topic_obama")
df_SentimentScore.filter(df_SentimentScore['Topic'] == 'economy').groupBy('Topic').agg(F.sum("SentimentScore").alias("Summary"))\
                    .coalesce(1).write.format("csv").options(header='true').save("output/Q3/"+"SentimentScore_Sum_Topic_economy")
df_SentimentScore.filter(df_SentimentScore['Topic'] == 'microsoft').groupBy('Topic').agg(F.sum("SentimentScore").alias("Summary"))\
                    .coalesce(1).write.format("csv").options(header='true').save("output/Q3/"+"SentimentScore_Sum_Topic_microsoft")
df_SentimentScore.filter(df_SentimentScore['Topic'] == 'palestine').groupBy('Topic').agg(F.sum("SentimentScore").alias("Summary"))\
                    .coalesce(1).write.format("csv").options(header='true').save("output/Q3/"+"SentimentScore_Sum_Topic_palestine")
df_SentimentScore.filter(df_SentimentScore['Topic'] == 'obama').groupBy('Topic').agg(F.mean("SentimentScore").alias("Average"))\
                    .coalesce(1).write.format("csv").options(header='true').save("output/Q3/"+"SentimentScore_Avg_Topic_obama")
df_SentimentScore.filter(df_SentimentScore['Topic'] == 'economy').groupBy('Topic').agg(F.mean("SentimentScore").alias("Average"))\
                    .coalesce(1).write.format("csv").options(header='true').save("output/Q3/"+"SentimentScore_Avg_Topic_economy")   
df_SentimentScore.filter(df_SentimentScore['Topic'] == 'microsoft').groupBy('Topic').agg(F.mean("SentimentScore").alias("Average"))\
                    .coalesce(1).write.format("csv").options(header='true').save("output/Q3/"+"SentimentScore_Avg_Topic_microsoft")
df_SentimentScore.filter(df_SentimentScore['Topic'] == 'palestine').groupBy('Topic').agg(F.mean("SentimentScore").alias("Average"))\
                    .coalesce(1).write.format("csv").options(header='true').save("output/Q3/"+"SentimentScore_Avg_Topic_palestine")

# (4) From subtask (1), for the top-100 frequent words per topic in titles and headlines, calculate their co-occurrence matrices (100x100), respectively. Each entry in the matrix will contain the co-occurrence frequency in all news titles and headlines, respectively
# #### Topic Obama in Title

#topic_obama_in_title
TTO = TitleWords.groupBy("Title","Topic").count().orderBy(desc("count")).where(TitleWords["Topic"]=="obama").withColumnRenamed("Title","Word")        
TopWordObama = TTO.select("Word")

TTO2 = df4_news.select("Title","Topic").filter(df4_news["Topic"]=="obama")
TTO3 = TTO2.select("Title")

#convert DF to RDD
title_obama = TTO3.rdd.flatMap(lambda x: x).collect()
topic_obama_title = TopWordObama.rdd.flatMap(lambda x: x).take(100)

co_occurrence_matrices=[[0]*100 for i in range(100)]
out=open('output/Q4/'+'obama_matrix_title.txt','w')
for i in range(100):
    for j in range(100):
        for k in range(len(title_obama)):
            if topic_obama_title[i] in title_obama[k] and topic_obama_title[j] in title_obama[k]:
                co_occurrence_matrices[i][j]+=1
        out.write(str(co_occurrence_matrices[i]) + '\n')


# #### Topic economy in Title
TTE = TitleWords.groupBy("Title","Topic").count().orderBy(desc("count")).where(TitleWords["Topic"]=="economy").withColumnRenamed("Title","Word")        
TopWordEconomy = TTE.select("Word")

TTE2 = df4_news.select("Title","Topic").filter(df4_news["Topic"]=="economy")
TTE3 = TTE2.select("Title")

title_economy = TTE3.rdd.flatMap(lambda x: x).collect()
topic_economy_title = TopWordEconomy.rdd.flatMap(lambda x: x).take(100)

co_occurrence_matrices=[[0]*100 for i in range(100)]
out=open('output/Q4/'+'economy_matrix_title.txt','w')
for i in range(100):
    for j in range(100):
        for k in range(len(title_economy)):
            if topic_economy_title[i] in title_economy[k] and topic_economy_title[j] in title_economy[k]:
                co_occurrence_matrices[i][j]+=1
        out.write(str(co_occurrence_matrices[i]) + '\n')

# #### Topic microsoft in Title
TTM = TitleWords.groupBy("Title","Topic").count().orderBy(desc("count")).where(TitleWords["Topic"]=="microsoft").withColumnRenamed("Title","Word")        
TopWordMicrosoft = TTM.select("Word")

TTM2 = df4_news.select("Title","Topic").filter(df4_news["Topic"]=="microsoft")
TTM3 = TTM2.select("Title")
#convert DF to RDD
title_microsoft = TTM3.rdd.flatMap(lambda x: x).collect()
topic_microsoft_title = TopWordMicrosoft.rdd.flatMap(lambda x: x).take(100)

co_occurrence_matrices=[[0]*100 for i in range(100)]
out=open('output/Q4/'+'microsoft_matrix_title.txt','w')
for i in range(100):
    for j in range(100):
        for k in range(len(title_microsoft)):
            if topic_microsoft_title[i] in title_microsoft[k] and topic_microsoft_title[j] in title_microsoft[k]:
                co_occurrence_matrices[i][j]+=1
        out.write(str(co_occurrence_matrices[i]) + '\n')


# #### Topic palestine in Title

TTP = TitleWords.groupBy("Title","Topic").count().orderBy(desc("count")).where(TitleWords["Topic"]=="palestine").withColumnRenamed("Title","Word")        
TopWordPalestine = TTP.select("Word")

TTP2 = df4_news.select("Title","Topic").filter(df4_news["Topic"]=="palestine")
TTP3 = TTP2.select("Title")

title_palestine = TTP3.rdd.flatMap(lambda x: x).collect()
topic_palestine_title = TopWordPalestine.rdd.flatMap(lambda x: x).take(100)

co_occurrence_matrices=[[0]*100 for i in range(100)]
out=open('output/Q4/'+'palestine_matrix_title.txt','w')
for i in range(100):
    for j in range(100):
        for k in range(len(title_palestine)):
            if topic_palestine_title[i] in title_palestine[k] and topic_palestine_title[j] in title_palestine[k]:
                co_occurrence_matrices[i][j]+=1
        out.write(str(co_occurrence_matrices[i]) + '\n')


# #### Topic Obama in Headline

HTO = HeadlineWords.groupBy("Headline","Topic").count()        .orderBy(desc("count")).where(HeadlineWords["Topic"]=="obama")        .withColumnRenamed("Headline","Word")        
TopWordObamaHL = HTO.select("Word")

HTO2 = df4_news.select("Headline","Topic").filter(df4_news["Topic"]=="obama")
HTO3 = HTO2.select("Headline")

headline_obama=HTO3.rdd.flatMap(lambda x: x).filter(lambda x:type(x)==str).collect()
topic_obama_headline = TopWordObamaHL.rdd.flatMap(lambda x: x).take(100)

co_occurrence_matrix=[[0]*100 for i in range(100)]
output=open('output/Q4/'+'obama_matrix_headline.txt','w')
for i in range(100):
    for j in range(100):
        for k in range(len(headline_obama)):
            if topic_obama_headline[i] in headline_obama[k] and topic_obama_headline[j] in headline_obama[k]:
                co_occurrence_matrix[i][j]+=1
        output.write(str(co_occurrence_matrix[i]) + '\n')

# #### Topic Economy in Headline

HTE = HeadlineWords.groupBy("Headline","Topic").count().orderBy(desc("count")).where(HeadlineWords["Topic"]=="economy").withColumnRenamed("Headline","Word")
TopWordEconomyHL = HTE.select("Word")

HTE2 = df4_news.select("Headline","Topic").filter(df4_news["Topic"]=="economy")
HTE3 = HTE2.select("Headline")

headline_economy = HTE3.rdd.flatMap(lambda x: x).filter(lambda x:type(x)==str).collect()
topic_economy_headline = TopWordEconomyHL.rdd.flatMap(lambda x: x).take(100)

co_occurrence_matrix=[[0]*100 for i in range(100)]
output=open('output/Q4/'+'economy_matrix_headline.txt','w')
for i in range(100):
    for j in range(100):
        for k in range(len(headline_economy)):
            if topic_economy_headline[i] in headline_economy[k] and topic_economy_headline[j] in headline_economy[k]:
                co_occurrence_matrix[i][j]+=1
        output.write(str(co_occurrence_matrix[i]) + '\n')


# #### Topic Microsoft in Headline

HTM = HeadlineWords.groupBy("Headline","Topic").count().orderBy(desc("count")).where(HeadlineWords["Topic"]=="microsoft").withColumnRenamed("Headline","Word")
TopWordMicrosoftHL = HTM.select("Word")

HTM2 = df4_news.select("Headline","Topic").filter(df4_news["Topic"]=="microsoft")
HTM3 = HTM2.select("Headline")

headline_microsoft = HTM3.rdd.flatMap(lambda x: x).filter(lambda x:type(x)==str).collect()
topic_microsoft_headline = TopWordMicrosoftHL.rdd.flatMap(lambda x: x).take(100)

co_occurrence_matrix=[[0]*100 for i in range(100)]
output=open('output/Q4/'+'microsoft_matrix_headline.txt','w')
for i in range(100):
    for j in range(100):
        for k in range(len(headline_microsoft)):
            if topic_microsoft_headline[i] in headline_microsoft[k] and topic_microsoft_headline[j] in headline_microsoft[k]:
                co_occurrence_matrix[i][j]+=1
        output.write(str(co_occurrence_matrix[i]) + '\n')


# #### Topic Palestine in Headline

HTP = HeadlineWords.groupBy("Headline","Topic").count().orderBy(desc("count")).where(HeadlineWords["Topic"]=="palestine").withColumnRenamed("Headline","Word")
TopWordPalestineHL = HTP.select("Word")

HTP2 = df4_news.select("Headline","Topic").filter(df4_news["Topic"]=="'palestine'")
HTP3 = HTP2.select("Headline")

headline_palestine = HTM3.rdd.flatMap(lambda x: x).filter(lambda x:type(x)==str).collect()
topic_palestine_headline = TopWordPalestineHL.rdd.flatMap(lambda x: x).take(100)

co_occurrence_matrix=[[0]*100 for i in range(100)]
output=open('output/Q4/'+'palestine_matrix_headline.txt','w')
for i in range(100):
    for j in range(100):
        for k in range(len(headline_palestine)):
            if topic_palestine_headline[i] in headline_palestine[k] and topic_palestine_headline[j] in headline_palestine[k]:
                co_occurrence_matrix[i][j]+=1
        output.write(str(co_occurrence_matrix[i]) + '\n')
