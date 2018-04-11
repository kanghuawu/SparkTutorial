import findspark
findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')
from pyspark import SparkContext, SparkConf


if __name__ == '__main__':
    conf = SparkConf().setAppName('take').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    words = ['spark', 'hadoop', 'spark', 'hive', 'pig', 'cassandra', 'hadoop']
    word_take = word_collections = sc.parallelize(words). \
        take(3)
    for w in word_take:
        print(w, end=' ')