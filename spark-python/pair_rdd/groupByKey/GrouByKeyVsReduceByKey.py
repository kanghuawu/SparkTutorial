import findspark
from pyspark import SparkContext, SparkConf

findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')

if __name__ == '__main__':

    conf = SparkConf().setAppName('groupByKey_vs_reduceByKey').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')

    words = ["one", "two", "two", "three", "three", "three"]

    pair_rdd = sc.parallelize(words) \
        .map(lambda w: (w, 1)) \
        .persist()

    word_count_reduceByKey = pair_rdd.reduceByKey(lambda x, y: x + y) \
        .collect()
    print('reduceByKey: {}'.format(word_count_reduceByKey))

    word_count_groupByKey = pair_rdd.groupByKey() \
        .mapValues(len) \
        .collect()
    print('groupByKey: {}'.format(word_count_groupByKey))