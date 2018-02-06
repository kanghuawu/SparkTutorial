import findspark
findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')
from pyspark.sql import SparkSession, functions as F

COUNTRY = 'country'
OCCUPATION = 'occupation'
AGE_MIDPOINT = "age_midpoint"
SALARY_MIDPOINT = "salary_midpoint"
SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket"

if __name__ == '__main__':
    session = SparkSession.builder.appName('stack_over_flow_survey').master('local[*]').getOrCreate()
    session.sparkContext.setLogLevel('ERROR')
    data_frame_reader = session.read

    res = data_frame_reader \
        .option('header', 'true') \
        .option('inferSchema', value=True)\
        .csv('../in/2016-stack-overflow-survey-responses.csv')


    print('=== Print out schema ===')
    res.printSchema()



    print('=== Print 20 records of responses table ===')
    res.show(20)

    print('=== Print the so_region and self_identification columns of gender table ===')
    res.select('so_region', 'self_identification').show()

    res_selected = res.select(COUNTRY, OCCUPATION, AGE_MIDPOINT, SALARY_MIDPOINT) \
        .persist()

    print('=== Print records where the response is from Afghanistan ===')
    res_selected.filter(res_selected[COUNTRY] == "Afghanistan").show()

    print('=== Print the count of occupations ===')
    res_selected.groupBy(res_selected[OCCUPATION])\
        .count()\
        .show()

    print('=== Print records with average mid age less than 20 ===')
    res_selected.filter(res_selected[AGE_MIDPOINT] < 20).show()

    print('=== Print the result by salary middle point in descending order ===')
    res_selected.orderBy(res_selected[SALARY_MIDPOINT], ascending=False).show()

    print('=== Group by country and aggregate by average salary middle point and max age middle point ===')
    res_selected.groupBy(COUNTRY)\
        .agg(F.avg(SALARY_MIDPOINT), F.min(SALARY_MIDPOINT), F.max(SALARY_MIDPOINT))\
        .show()

    res_with_salary_bucket = res.withColumn(SALARY_MIDPOINT_BUCKET, ((res[SALARY_MIDPOINT]/20000).cast('integer')*2000))

    print('=== With salary bucket column ===')
    res_with_salary_bucket.select(SALARY_MIDPOINT, SALARY_MIDPOINT_BUCKET).show()

    print('=== Group by salary bucket ===')
    res_with_salary_bucket.groupBy(SALARY_MIDPOINT_BUCKET).count().orderBy(SALARY_MIDPOINT_BUCKET).show()
    session.stop()