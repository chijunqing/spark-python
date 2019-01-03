from __future__ import print_function
from pyspark.sql import Row, SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *


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
    # if len(sys.argv) != 4:
    #     print("""
    #     Usage: SparkTest-sql-kafka-.py <bootstrap-servers> <subscribe-type> <topics>
    #     """, file=sys.stderr)
        # exit(-1)


    subscribeType = "subscribe"
    bootstrapServers = "10.0.10.10:2181"
    # 打开一个TCP socket 地址 和 端口号
    topic = {"test5": 1}  # 要列举出分区
    groupid = "sprk-consumer-group"

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    # 处理时间间隔为2s
    ssc = StreamingContext(sc, 15)


    kafkaStream = KafkaUtils.createStream(ssc, bootstrapServers, groupid, topic)
    words = kafkaStream.map(lambda x: x[1])
    words.pprint();

    # .trigger(processingTime='5 seconds') \
  # Convert RDDs of the words DStream to DataFrame and run SQL query
    def process(time, rdd):
        print("========= %s =====执行时间====" % str(time))

        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            schemaString = "c1 c2 c3"
            fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(" ")]
            schema = StructType(fields)

            rowRdd2 = rdd.map(lambda line: line.split(","))
            # rowRdd3 = rowRdd2.map(lambda w: Row(c1=w[0] ,c2=w,c3=w))
            rowRdd3 = rowRdd2.map(lambda w: (w[0] ,w[1],w[2]))
            wordsDataFrame = spark.createDataFrame(rowRdd3,schema=schema)
            wordsDataFrame.show();
            # Creates a temporary view using the DataFrame.
            wordsDataFrame.createOrReplaceTempView("words")
            # Do word count on table using SQL and print it
            wordCountsDataFrame = spark.sql("select count(*)  from words ")
            wordCountsDataFrame.show()
        except BaseException as e:
            print(e);

    words.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
