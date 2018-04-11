import findspark
findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')
from pyspark import SparkContext, SparkConf


if __name__ == '__main__':
    conf = SparkConf().setAppName('reduce').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    num = [x for x in range(1, 5)]
    product  = sc.parallelize(num). \
        reduce(lambda x, y: x*y)

    print(product)