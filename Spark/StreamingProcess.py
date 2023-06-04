# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from kafka import KafkaConsumer
import json
from pyspark.sql.window import Window

# COMMAND ----------

brokers = 'b-1.detrainingmsk.66lq6h.c10.kafka.us-east-1.amazonaws.com:9092'
topic = 'coin_cap_data'


# COMMAND ----------

#read streaming data from kafka

df_rawData = (spark.readStream.format('kafka')
           .option('inferSchema',True)
           .option('kafka.bootstrap.servers', brokers)
           .option('subscribe', topic)
           .option('startingOffsets', 'latest')
           .load()
             )

df_rawData.printSchema()
display(df_rawData)

# COMMAND ----------

#define schema
schema = StructType([
                    StructField('exchange',StringType()),
                    StructField('base',StringType()),
                    StructField('quote',StringType()),
                    StructField('direction',StringType()),
                    StructField('price',DoubleType()),
                    StructField('volume',DoubleType()),
                    StructField('timestamp',LongType()),
                    StructField('priceUsd',DoubleType())
          ])

# COMMAND ----------

#cast raw data as string, split columns in message value 
df_splitted = (df_rawData.select(from_json(df_rawData.value.cast('string'), schema).alias('data'))
                      .select('data.*')
           )
df_splitted.printSchema()
display(df_splitted)

# COMMAND ----------

#process timestamp
df = (df_splitted.withColumn('timestamp', from_unixtime(df_splitted.timestamp/1000, 'yyyy-MM-dd HH:mm:ss'))
                                 .withColumn('timestamp', from_utc_timestamp('timestamp', 'UTC'))
                      )
df.printSchema()
display(df)

# COMMAND ----------

#calcualte average price in tumbling window of 1 minute 
tumbling_window = (df.withWatermark('timestamp', '5 minutes')
                     .withColumn('amount', df.price * df.volume)
                     .groupBy('base', window('timestamp', '1 minutes').alias('window'))
                     .agg(sum('volume').alias('total_volume'),
                          sum('amount').alias('total_amount'))
                     .withColumn('avg_price', col('total_amount') / col('total_volume'))
                     .select('window', 'base', 'total_amount', 'total_volume', 'avg_price')
                    )
tumbling_window.printSchema()
display(tumbling_window)

# COMMAND ----------

#save streaming data to memory 
tumbling_window_write = (tumbling_window.writeStream
                                        .queryName('tumbling_window')
                                        .trigger(processingTime = '10 seconds')
                                        .format('memory')
                                        .outputMode('complete')
                                        .start()
                         )


# COMMAND ----------

#only show price trend in the lastest 5 minutes
five_min_window = spark.sql('select * from tumbling_window')

price_windowSpec = Window.partitionBy('base').orderBy('window')
rowId_windowSpec = Window.partitionBy('base').orderBy(desc('window'))

five_min_trend = (five_min_window.withColumn('trend', when(col('avg_price') > lag('avg_price').over(price_windowSpec), 'Up')
                                            .when(col('avg_price') == lag('avg_price').over(price_windowSpec), 'Flat')
                                            .when(col('avg_price') < lag('avg_price').over(price_windowSpec), 'Down')
                                            .otherwise('N/a'))
                                 .withColumn('rowId', row_number().over(rowId_windowSpec))
                                 .where(col('rowId') <= 5)
                                 .orderBy('base', 'window')
                 )
display(five_min_trend)

# COMMAND ----------

#if we want to see the trend multiple times 
for i in range(10):
    print(f'Run index {i}:')
    five_min_window = spark.sql('select * from tumbling_window')

    price_windowSpec = Window.partitionBy('base').orderBy('window')
    rowId_windowSpec = Window.partitionBy('base').orderBy(desc('window'))

    five_min_trend = (five_min_window.withColumn('trend', when(col('avg_price') > lag('avg_price').over(price_windowSpec), 'Up')
                                                .when(col('avg_price') == lag('avg_price').over(price_windowSpec), 'Flat')
                                                .when(col('avg_price') < lag('avg_price').over(price_windowSpec), 'Down')
                                                .otherwise('N/a'))
                                     .withColumn('rowId', row_number().over(rowId_windowSpec))
                                     .where(col('rowId') <= 5)
                                     .orderBy('base', 'window')
                     )
    five_min_trend.show(20, truncate = False)
    time.sleep(60)

# COMMAND ----------

#save streaming data process results to Kafka for future use
avg_price_checkpoint = '/users/peekaboo15/checkpoint/coin_cap/avg_price'

avg_price_save = (tumbling_window.writeSTREAM
                                    .queryName('kafka_avg_price')
                                    .trigger(processingTime = '1 minute')
                                    .format('kafka')
                                    .option('kafka.bootstrap.servers', brokers)
                                    .option('topic', topic)
                                    .ouputMode('complete')
                                    .option('checkpointLocation', avg_price_checkpoint)
                                    .start()
                )

price_trend_checkpoint = '/users/peekaboo15/checkpoint/coin_cap/price_trend'

price_trend_save = (five_min_trend.writeSTREAM
                                    .queryName('kafka_avg_price')
                                    .trigger(processingTime = '1 minute')
                                    .format('kafka')
                                    .option('kafka.bootstrap.servers', brokers)
                                    .option('topic', topic)
                                    .ouputMode('complete')
                                    .option('checkpointLocation', price_trend_checkpoint)
                                    .start()
                )
