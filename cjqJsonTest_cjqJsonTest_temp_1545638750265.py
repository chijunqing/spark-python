# -*- coding: utf-8 -*-
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
import json
from kafka import KafkaProducer

#初始化
def init():
    sc = SparkContext(appName="cjq")
    # 初始化Spark上下文
    ssc = StreamingContext(sc, 30)
    return ssc

# 定义数据输入-内容根据配置动态替换
def inputDataSource(ssc):
    bootstrapServers = "10.0.8.107:6667,10.0.8.109:6667,10.0.8.110:6667"
    topic = ["test1"]
    groupid = "cjqJsonTest"
    kafkaParams ={"bootstrap.servers":bootstrapServers,"group.id":groupid}
    kafkaStream = KafkaUtils.createDirectStream(ssc, topic,kafkaParams)
    dStream = kafkaStream.map(lambda x: x[1])
    print("kafka 接收到数据：")
    dStream.pprint();
    return dStream;
    

def outputDataSource(dataFrame):
    #dataFrame=dataFrame.toJSON()
    dataFrame=dataFrame.toJSON();
    dataFrame.foreach(sendData);
    

# 定义数据输出-内容根据配置动态替换
def sendData(message):
    producer = KafkaProducer(bootstrap_servers=[10.0.8.107:6667,10.0.8.109:6667,10.0.8.110:6667],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    print("kafka 发送数据：")
    producer.send('test2', message);
    producer.flush();
    
    


#定义数据结构并将rdd转换dataFrame-内容根据配置动态替换
def getDataFrame(spark,rdd):
    schemaString = "c1 c2"
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(" ")]
    schema = StructType(fields)
    #dataFrame = spark.read.schema(schema).json(rdd);
    #return dataFrame
    dataFrame = spark.read.schema(schema).json(rdd)
    return dataFrame;
    

#执行sql 查询，过滤数据-内容根据配置动态替换
def sql_select(spark,rdddata):
    rdddata.createOrReplaceTempView("cjqJsonTest_temp")
    dataFrame = spark.sql("select c1,c2 from cjqJsonTest_temp ")
    return dataFrame


#数据处理流程-模板固定内容
def process(time, rdd):
    print("========= %s =====执行时间====" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())
        dataFrame=getDataFrame(spark,rdd)
        resultDataFrame =sql_select(spark, dataFrame);
        outputDataSource(resultDataFrame);
    except BaseException as e:
        print(e);

#模板中的内容-固定
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

#模板固定内容
if __name__ == "__main__":
    #初始化上下文
    ssc=init();
    #获取数据源
    words=inputDataSource(ssc);
    #数据处理
    words.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()

