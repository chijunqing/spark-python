from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == "__main__":
    # if len(sys.argv) != 4:
    #     print("""
    #     Usage: SparkTest-sql-kafka-.py <bootstrap-servers> <subscribe-type> <topics>
    #     """, file=sys.stderr)
        # exit(-1)

    bootstrapServers = "10.0.10.10:6667"
    subscribeType = "subscribe"
    topics = "test5"


    spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()

    # Create DataSet representing the stream of input lines from kafka
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, topics) \
        .option("value.serializer", "org.common.serialization.StringSerializer") \
        .option("key.serializer", "org.common.serialization.StringSerializer") \
        .load()\
        .selectExpr("CAST(value AS STRING)")
    print("lines type:",type(lines))


    # Split the lines into words

    print(lines.foreach);

    words = lines.select(
        # explode turns each item in an array into a separate row
        explode(
            split(lines.value, ' ')
        ).alias('word')
    )

    # Generate running word count
    wordCounts = words.groupBy('word').count()

    # Start running the query that prints the running counts to the console
    # df.createOrReplaceTempView("people")

    # sqlDF = spark.sql("SELECT * FROM people")
    # print(type(sqlDF))
    # sqlDF.show()



    # 输出======================================================================
    query = wordCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()

    query.awaitTermination()





