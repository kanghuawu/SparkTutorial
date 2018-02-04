import findspark
from pyspark import SparkContext, SparkConf

findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')

if __name__ == '__main__':
    conf = SparkConf().setAppName('join').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    ages = sc.parallelize([("Tom", 29), ("John", 22)]).persist()
    addresses = sc.parallelize([("James", "USA"), ("John", "UK")]).persist()

    ages.join(addresses) \
        .coalesce(1) \
        .saveAsTextFile('../../out/age_address_join.text')

    ages.leftOuterJoin(addresses) \
        .coalesce(1) \
        .saveAsTextFile('../../out/age_address_left_out_join.text')

    ages.rightOuterJoin(addresses) \
        .coalesce(1) \
        .saveAsTextFile('../../out/age_address_right_out_join.text')

    ages.fullOuterJoin(addresses) \
        .coalesce(1) \
        .saveAsTextFile('../../out/age_address_full_out_join.text')