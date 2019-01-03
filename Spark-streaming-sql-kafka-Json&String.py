# -*- coding: utf-8 -*-
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
import json
from kafka import KafkaProducer
from kazoo.client import KazooClient



def get_zookeeper_instance():
    ZOOKEEPER_SERVERS = "10.0.8.107:2181"
    if 'KazooSingletonInstance' not in globals():
        globals()['KazooSingletonInstance'] = KazooClient(ZOOKEEPER_SERVERS)
        globals()['KazooSingletonInstance'].start()
    return globals()['KazooSingletonInstance']

def get_kafka_producer_instance():
    if 'KafkaProducerInstance' not in globals():
        globals()['KafkaProducerInstance'] = KafkaProducer(
            bootstrap_servers=['sdc403.sefonsoft.com:6667', 'sdc404.sefonsoft.com:6667', 'sdc405.sefonsoft.com:6667'],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'))
    return globals()['KafkaProducerInstance']

def save_offsets(rdd):
    zk = get_zookeeper_instance()
    for offset in rdd.offsetRanges():
        path = f"/consumers/{offset.topic}/{offset.partition}"
        zk.ensure_path(path)
        zk.set(path, str(offset.untilOffset).encode())


def read_offsets(ssc,zk, topics,kafkaParams):
    from pyspark.streaming.kafka import TopicAndPartition
    print("zk===", zk)
    print("topics===", topics)
    from_offsets = {}
    try:
        for topic in topics:
            print("topic=====",topic)
            for partition in zk.get_children(f'/consumers/{topic}'):
                topic_partion = TopicAndPartition(topic, int(partition))
                offset = int(zk.get(f'/consumers/{topic}/{partition}')[0])
                from_offsets[topic_partion] = offset
    except Exception as e:
        print("read offset error=============",e)

    print("===from_offsets=====", from_offsets)
    return from_offsets



# def getDefaultOffset(ssc,topics,kafkaParams):
#     from pyspark.streaming.kafka import TopicAndPartition
#     from_offsets = {}
#     kafkaStream = KafkaUtils.createDirectStream(ssc, topics, kafkaParams)
#     kafkaStream.foreachRDD(saveFirstOffset)

    #
    # zk = get_zookeeper_instance()
    # for topic in topics:
    #     for partition in zk.get_children(f'/consumers/{topic}'):
    #         topic_partion = TopicAndPartition(topic, int(partition))
    #         offset = int(zk.get(f'/consumers/{topic}/{partition}')[0])
    #         from_offsets[topic_partion] = offset
    # return from_offsets;
#
# def saveFirstOffset(rdd):
#     print("===初次消费kafka======", rdd)
#     zk = get_zookeeper_instance()
#     for offset in rdd.offsetRanges():
#         path = f"/consumers/{offset.topic}/{offset.partition}"
#         zk.ensure_path(path)
#         zk.set(path, str(0).encode())

# 初始化
def init():
    sc = SparkContext(appName="UserInfoTest")
    # 初始化Spark上下文
    ssc = StreamingContext(sc, 30)
    return ssc


# 定义数据输入-内容根据配置动态替换
def inputDataSource(ssc):
    zk = get_zookeeper_instance()
    bootstrapServers = "sdc403.sefonsoft.com:6667,sdc404.sefonsoft.com:6667,sdc405.sefonsoft.com:6667"
    topic = ["source"]
    groupid = "UserInfoTest2"
    kafkaParams = {"bootstrap.servers": bootstrapServers, "group.id": groupid}
    from_offsets = read_offsets(ssc,zk, topic,kafkaParams)
    kafkaStream = KafkaUtils.createDirectStream(ssc, topic, kafkaParams,fromOffsets=from_offsets)
    dStream = kafkaStream.map(lambda x: x[1])
    print("kafka 接收到数据：")
    dStream.pprint();
    kafkaStream.foreachRDD(save_offsets)
    return dStream;


def outputDataSource(dataFrame):
    # dataFrame=dataFrame.toJSON()
    dataFrame.foreach(sendData);


# 定义数据输出-内容根据配置动态替换
def sendData(message):
    # producer = KafkaProducer(
    #     bootstrap_servers=['sdc403.sefonsoft.com:6667', 'sdc404.sefonsoft.com:6667', 'sdc405.sefonsoft.com:6667'],
    #     value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'))
    print("kafka 发送数据：")
    producer=get_kafka_producer_instance();
    print("kafka 发送数据：")
    producer.send('target', message);
    producer.flush();


# 定义数据结构并将rdd转换dataFrame-内容根据配置动态替换
def getDataFrame(spark, rdd):
    schemaString = "name age"
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(" ")]
    schema = StructType(fields)
    # dataFrame = spark.read.schema(schema).json(rdd);
    # return dataFrame
    dataFrame = spark.read.schema(schema).json(rdd)
    return dataFrame;


# 执行sql 查询，过滤数据-内容根据配置动态替换
def sql_select(spark, rdddata):
    rdddata.createOrReplaceTempView("UserInfoTest_temp")
    dataFrame = spark.sql("select name,age from UserInfoTest_temp where  age>=18 ")
    return dataFrame


# 数据处理流程-模板固定内容
def process(time, rdd):
    print("========= %s =====执行时间====" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())
        dataFrame = getDataFrame(spark, rdd)
        resultDataFrame = sql_select(spark, dataFrame);
        outputDataSource(resultDataFrame);
    except BaseException as e:
        print(e);


# 模板中的内容-固定
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]



# 模板固定内容
if __name__ == "__main__":
    # 初始化上下文
    ssc = init();
    # 获取数据源
    rdd = inputDataSource(ssc);
    # 数据处理
    if(rdd !=None):
        rdd.foreachRDD(process)
    else:
        print("rdd is null=====")
    ssc.start()
    ssc.awaitTermination()

