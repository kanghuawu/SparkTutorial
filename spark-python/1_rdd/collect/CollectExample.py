import findspark
findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')
from pyspark import SparkContext, SparkConf


if __name__ == '__main__':
    conf = SparkConf().setAppName('collect').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    words = ['spark', 'hadoop', 'spark', 'hive', 'pig', 'cassandra', 'hadoop']
    word_collections = sc.parallelize(words) \
        .collect()

    word_distinct = sc.parallelize(words) \
        .distinct() \
        .collect()

    for w in word_collections:
        print(w, end=' ')
    print()
    for w in word_distinct:
        print(w, end=' ')