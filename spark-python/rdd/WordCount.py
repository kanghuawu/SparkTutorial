import findspark
findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')
from pyspark import SparkContext, SparkConf


if __name__ == '__main__':
    conf = SparkConf().setAppName('wordcount').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    word_count = sc.textFile('../in/word_count.text') \
        .flatMap(lambda line: line.split(' ')) \
        .countByValue()

    for word, count in word_count.items():
        print('{} : {}'.format(word, count))



