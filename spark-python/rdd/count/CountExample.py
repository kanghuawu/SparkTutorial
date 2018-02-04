import findspark
findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')
from pyspark import SparkContext, SparkConf


if __name__ == '__main__':
    conf = SparkConf().setAppName('count').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    words = ['spark', 'hadoop', 'spark', 'hive', 'pig', 'cassandra', 'hadoop']
    word_rdd = word_collections = sc.parallelize(words)

    print("Count: {}".format(word_rdd.count()))

    word_count_by_value = word_rdd.countByValue()

    print('CountByValue:')

    for w, c in word_count_by_value.items():
        print('{} : {}'.format(w, c))