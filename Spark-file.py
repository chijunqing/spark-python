# -*- coding: utf-8 -*
from pyspark import SparkConf, SparkContext

def g(x):
    print(x);
sc = SparkContext("local", "python-wordcount")

# 读取数据
textFile = sc.textFile("file:///F:\\ai\\python\\work\\spark\\test.txt")

# -----------逻辑处理-begin-------
# 数据转化为RDD -Resilient Distributed Dataset，弹性分布式数据集
splitRDD = textFile.flatMap(lambda line: line.split(" "));
mapRDD=splitRDD.map(lambda word: (word, 1))
reduceRDD= mapRDD.reduceByKey(lambda a, b: a+b)
# --------------逻辑处理-end----------------

# 输出 统计结果
reduceRDD.foreach(g);
# 停止服务
sc.stop();



