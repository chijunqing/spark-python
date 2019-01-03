from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import split

if __name__ == "__main__":

    bootstrapServers = "10.0.10.10:6667"
    subscribeType = "subscribe"
    topics = "test5"


    spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()

    # Create DataSet representing the stream of input lines from kafka
    data = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers) \
        .option("value.serializer", "org.common.serialization.StringSerializer") \
        .option("key.serializer", "org.common.serialization.StringSerializer") \
        .option(subscribeType, topics) \
        .option("startingOffsets", "latest") \
        .load()
        # .selectExpr("CAST(value AS STRING)")

    print("lines type:",type(data))


    id_DF = data.select(split(data.value, ",").getItem(0).alias("col1"),
                        split(data.value, ",").getItem(1).alias("col2"))
    print(type(id_DF))

    id_DF.createOrReplaceTempView("ds")

    wordCounts = spark.sql("Select col1, col2  from ds")
    query = wordCounts.writeStream.format("console").start()
    spark.streams.awaitAnyTermination()
    # .trigger(processingTime='5 seconds') \

