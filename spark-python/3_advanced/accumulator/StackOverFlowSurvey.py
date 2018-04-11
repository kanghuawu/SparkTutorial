import findspark
from pyspark import SparkContext, SparkConf
from common.Utils import Utils

findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')


def accumulator_helper(line, total, missing, bytes_processed):
    bytes_processed.add(len(line.encode('utf-8')))
    splits = Utils.COMMA_DELIMITER.split(line)
    total.add(1)
    if not splits[14]:
        missing.add(1)
    return splits[2] == 'Canada'


if __name__ == '__main__':
    conf = SparkConf().setAppName('stackoverflow').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')

    total = sc.accumulator(0)
    missing = sc.accumulator(0)
    bytes_processed = sc.accumulator(0)

    from_canada = sc.textFile('../in/2016-stack-overflow-survey-responses.csv') \
        .filter(lambda line: accumulator_helper(line, total, missing, bytes_processed)) \
        .count()

    print('From Canada: {}'.format(from_canada))
    print('Total count of response: {}'.format(total))
    print('Missing salary middle point: {}'.format(missing))
    print('Bytes processed: {}'.format(bytes_processed))