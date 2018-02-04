import findspark
from pyspark import SparkContext, SparkConf

findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')

if __name__ == '__main__':
    conf = SparkConf().setAppName('word_count').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    res = sc.textFile('../../../in/word_count.text') \
        .flatMap(lambda line: line.split(' ')) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .collect()

    for w, c in res:
        print(w, c)