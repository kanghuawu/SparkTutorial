import findspark
findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')
from pyspark import SparkContext, SparkConf


if __name__ == '__main__':
    '''
    Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
    print the sum of those numbers to console.
    Each row of the input file contains 10 prime numbers separated by spaces.
    '''

    conf = SparkConf().setAppName('sum_of_numbers').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    sum = sc.textFile('../../in/prime_nums.text') \
        .flatMap(lambda line: line.split('\t')) \
        .filter(lambda n: n) \
        .map(lambda n: int(n)) \
        .reduce(lambda x, y: x+y)

    print(sum)
