# -*- coding: utf-8 -*
from pyspark import SparkConf, SparkContext

def g(x):
    print(x);
sc = SparkContext("","python-wordcount")

# 需要处理的数据准备
data = ["hello","word","test","word","hello","hello","word","test","word","hello","hello","word","test","word","hello","hello","word","test","word","hello","hello","word","test","word","hello","hello","word","test","word","hello","hello","word","test","word","hello","hello","word","test","word","hello","hello","word","test","word","hello","hello","word","test","word","hello","hello","word","test","word","hello","hello","word","test","word","hello","hello","word","test","word","hello","hello","word","test","word","hello","hello","word","test","word","hello","hello","word","test","word","hello"];


# -----------逻辑处理-begin-------
# 数据转化为RDD -Resilient Distributed Dataset，弹性分布式数据集
splitValues=sc.parallelize(data,10);
# Word 分散
splitValues = splitValues.flatMap(lambda line: line.split(" "))
# 使用map 创建 Key - value 形式对
words=splitValues.map(lambda word: (word, 1))
# 统计每个单词出现次数
counts=words.reduceByKey(lambda a, b: a + b)
# --------------逻辑处理-end----------------

# 输出 统计结果
counts.foreach(g);

# 停止服务
sc.stop();



