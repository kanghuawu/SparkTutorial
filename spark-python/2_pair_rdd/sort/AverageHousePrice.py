import findspark
from pyspark import SparkContext, SparkConf

findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')

if __name__ == '__main__':

    conf = SparkConf().setAppName('real_estate').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    combiner = lambda x: (x, 1)
    merge_value = lambda x, y: (x[0] + y, x[1] + 1)
    merge_combiner = lambda x, y: (x[0] + y[0], x[1] + y[1])
    res = sc.textFile('../../in/RealEstate.csv') \
        .filter(lambda line: not line.startswith('MLS,Location')) \
        .map(lambda line: (int(line.split(',')[3]), float(line.split(',')[2]))) \
        .combineByKey(combiner, merge_value, merge_combiner) \
        .mapValues(lambda x: x[0] / x[1]) \
        .sortByKey() \
        .collect()

    for k, v in res:
        print('({}, {:.2f})'.format(k, v))