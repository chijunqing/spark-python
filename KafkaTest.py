from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
import json
from kafka import KafkaProducer
from kazoo.client import KazooClient
def sendData(message):
    message2=bytes(message, encoding="utf8")
    producer = KafkaProducer(bootstrap_servers=['10.0.8.107:6667'],value_serializer = lambda v: json.dumps(v ,ensure_ascii = False).encode("utf-8"))
    print("kafka 发送数据：")
    producer.send('target', message2);
    producer.flush();


def get_zookeeper_instance():
    ZOOKEEPER_SERVERS = "10.0.8.107:2181"
    if 'KazooSingletonInstance' not in globals():
        globals()['KazooSingletonInstance'] = KazooClient(ZOOKEEPER_SERVERS)
        globals()['KazooSingletonInstance'].start()
    return globals()['KazooSingletonInstance']

def getDefaultOffset(ssc,topics, kafkaParams):
    from pyspark.streaming.kafka import TopicAndPartition
    from_offsets = {}
    print("===启动==")
    kafkaStream = KafkaUtils.createDirectStream(ssc, topics, kafkaParams,from_offsets)
    kafkaStream.foreachRDD(saveFirstOffset)

def saveFirstOffset(rdd):
    print("===初次消费kafka======", rdd)
    zk = get_zookeeper_instance()
    for offset in rdd.offsetRanges():
        path = f"/consumers/{offset.topic}/{offset.partition}"
        zk.ensure_path(path)
        zk.set(path, str(offset.untilOffset).encode())

# 初始化
def init():
    sc = SparkContext(appName="UserInfoTest")
    # 初始化Spark上下文
    ssc = StreamingContext(sc, 30)
    return ssc
#模板固定内容
if __name__ == "__main__":
    # sendData("{\"aa\":\"中文222\"}");
    ssc=init();
    topics=[];
    bootstrapServers = "sdc403.sefonsoft.com:6667,sdc404.sefonsoft.com:6667,sdc405.sefonsoft.com:6667"
    topic = ["source"]
    groupid = "UserInfoTest2"
    kafkaParams = {"bootstrap.servers": bootstrapServers, "group.id": groupid}
    getDefaultOffset(ssc,topics,kafkaParams);
    ssc.start()
    ssc.awaitTermination()







