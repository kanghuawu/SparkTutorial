import findspark
import numpy as np
findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')
from pyspark import SparkContext, SparkConf, StorageLevel


if __name__ == '__main__':
    conf = SparkConf().setAppName('reduce').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')

    nums = np.arange(1, 11)
    persist = sc.parallelize(nums) \
        .persist(StorageLevel.MEMORY_ONLY)

    print('product: {}'.format(persist.reduce(lambda x, y: x * y)))

    print('sum: {}'.format(persist.reduce(lambda x, y: x + y)))