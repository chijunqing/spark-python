# -*- coding: utf-8 -*-
from __future__ import print_function
from pyspark.sql import Row, SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from pyspark.sql.types import *
import yaml
import json


def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def printRdd(x):
    return "python" in x



if __name__ == "__main__":

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    # 处理时间间隔为2s
    ssc = StreamingContext(sc, 15)


    # subscribeType = "subscribe"
    bootstrapServers = "10.0.10.10:2181"
    topic = {"test5": 1}  # 要列举出分区
    groupid = "sprk-consumer-group"
    kafkaStream = KafkaUtils.createStream(ssc, bootstrapServers, groupid, topic)
    words = kafkaStream.map(lambda x: x[1])


    def kafkProduct(message):
        producer = KafkaProducer(bootstrap_servers=['10.0.10.10:6667'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        print("message==type:",type(message))

        # msg = yaml.safe_load('{"id":1, "name":"oguz"}')
        producer.send('test7', message)
        producer.flush()

    def process(time, rdd):
        print("========= %s =====执行时间====" % str(time))

        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            schemaString = "c1 c2 c3"
            fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(" ")]
            schema = StructType(fields)

            wordsDataFrame= spark.read.schema(schema).json(rdd);
            wordsDataFrame.createOrReplaceTempView("words")

            wordCountsDataFrame = spark.sql("select *  from words ")
            wordCountsDataFrame.show();
            # wordCountsDataFrame .write.json("f:/a.txt");
            # producer.send("test6", wordCountsDataFrame.clloct())

            wordCountsDataFrame.foreach(kafkProduct)
            # wordCountsDataFrame\
            # .writeStream\
            # .foreach(writer)\
            # .outputMode("update")\
            # .start()

        except BaseException as e:
            print(e);

    words.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
