from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql.window import Window


# data loading
spark = SparkSession.builder.appName('test').getOrCreate()
country = spark.read.csv('/user/bryan/weatherdata/countrylist.csv', header=True)
station = spark.read.csv('/user/bryan/weatherdata/stationlist.csv', header=True)
weather = spark.read.csv('/user/bryan/weatherdata/weather/*', header=True)

# data preparation
country_station = station.join(country, 'COUNTRY_ABBR', 'left')
weather = weather.withColumnRenamed('STN---', 'STN_NO')
df = weather.join(country_station, 'STN_NO', 'left')
df = df.filter(df.COUNTRY_FULL.isNotNull() & df.YEARMODA.isNotNull())
df = df.withColumn('date', f.to_date(df.YEARMODA, 'yyyyMMdd'))


# which country had hottest average mean temperature over the year
df = df.filter(df.TEMP.isNotNull())
df.groupBy('COUNTRY_FULL').agg({'TEMP':'avg'}).orderBy(f.col('avg(TEMP)').desc()).head(1)



# which country had most consecutive days of tornados/funnel cloud formations?
df = df.filter(df.FRSHTT.isNotNull())
df = df.withColumn('tc', f.substring(df.FRSHTT, 6, 1).cast('int'))
df1 = df.groupBy('date', 'COUNTRY_FULL').agg({'tc':'max'}).withColumnRenamed('max(tc)', 'torc')
df1 = df1.filter(df1.torc == 0).orderBy('COUNTRY_FULL', 'date').withColumn('next_date', f.lead(df1.date).over(Window.partitionBy('COUNTRY_FULL').orderBy('date')))
df1 = df1.withColumn('date_diff', f.datediff(f.col('next_date'), f.col('date')))
df1.groupBy('COUNTRY_FULL').agg({'date_diff':'max'}).orderBy(f.col('max(date_diff)').desc()).head(1)


#which country had second highest average mean wind speed over the year?
df = df.filter(df.WDSP.isNotNull())
df.groupBy('COUNTRY_FULL').agg({'WDSP': 'avg'}).orderBy(f.col('avg(WDSP)').desc()).head(2).tail(1)    # tail function is available in spark 3
