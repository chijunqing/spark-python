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
    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    # 初始化Spark上下文
    ssc = StreamingContext(sc, 15)
    return ssc

# 定义数据输入-内容根据配置动态替换
def inputDataSource(ssc):
    bootstrapServers = "10.0.10.10:2181"
    topic = {"test5": 1}  # 要列举出分区
    groupid = "sprk-consumer-group"
    kafkaStream = KafkaUtils.createStream(ssc, bootstrapServers, groupid, topic)
    dStream = kafkaStream.map(lambda x: x[1])
    print("kafka 接收到数据：")
    dStream.pprint();
    return dStream;

# 定义数据输出-内容根据配置动态替换
def outputDataSource(message):
    producer = KafkaProducer(bootstrap_servers=['10.0.10.10:6667'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    print("message==type:", type(message))
    producer.send('test7', message)
    producer.flush()

#定义数据结构并将rdd转换dataFrame-内容根据配置动态替换
def getDataFrame(spark,rdd):
    schemaString = "c1 c2 c3"
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(" ")]
    schema = StructType(fields)
    dataFrame = spark.read.schema(schema).json(rdd);
    return dataFrame

#执行sql 查询，过滤数据-内容根据配置动态替换
def sql_select(spark,rdddata):
    rdddata.createOrReplaceTempView("words")
    dataFrame = spark.sql("select *  from words ")
    return dataFrame


#数据处理流程-模板固定内容
def process(time, rdd):
    print("========= %s =====执行时间====" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())
        dataFrame=getDataFrame(spark,rdd)
        resultDataFrame =sql_select(spark, dataFrame);
        resultDataFrame.show();
        resultDataFrame.foreach(outputDataSource)
    except BaseException as e:
        print(e);

#模板中的内容-固定
def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

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

