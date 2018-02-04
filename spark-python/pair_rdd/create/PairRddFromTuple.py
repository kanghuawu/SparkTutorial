import findspark
from pyspark import SparkContext, SparkConf
from common.Utils import Utils

findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')


def split_comma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return (splits[2], splits[3])

if __name__ == "__main__":
    conf = SparkConf().setAppName('create').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    tuples = [("Lily", 23), ("Jack", 29), ("Mary", 29), ("James", 8)]
    test = sc.parallelize(tuples) \
        .coalesce(1) \
        .saveAsTextFile('../../out/pair_rdd_from_tuple_list.text')