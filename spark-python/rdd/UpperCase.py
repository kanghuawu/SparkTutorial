import findspark
findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')
from pyspark import SparkContext, SparkConf


if __name__ == '__main__':
    conf = SparkConf().setAppName('wordcount').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    sc.textFile('../in/uppercase.text') \
        .map(lambda line: str(line).upper()) \
        .saveAsTextFile('../out/uppercase.text')