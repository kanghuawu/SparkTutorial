# Spark with Java 8

## Topics:

* RDD
  * `JavaRDD<T>`: `JavaRDD<Integer>`
  * Transformation
    * Basic
      * **`filter`**
      * **`map`**
      * **`flatMap`**
      * **`sample`**
      * **`distinct`**
    * Two RDDs
      * **`union`**
      * **`intersection`**
      * **`subtract`**
      * `cartesian`
  * Actions
    * Basic
      * `collect`
      * **`count`**
      * **`countByValue`**
      * **`take`**
      * `takeSample`
      * `takeOrdered`
      * **`reduce`**
      * **`foreach`**
      * `aggregate`
      * `fold`
* PairRdd
  * `scala.Tuple2`
  * Transformation
    * Basic
      * **`reduceByKey`**: `groupByKey` + (`reduce`, `map`, `mapValues`)
      * `groupbykey`
      * `combineBy`
      * **`mapValues`**
      * `keys`
      * `values`
      * **`sortByKey`**
    * Two RDDs
      * **`join`**
      * **`rightOuterJoin`**
      * **`leftOuterJoin`**
      * **`subtractByKey`**
      * `cogroup`
    * Advanced
      * `paritionBy`
  * Actions
    * Basic
      * **`countByKey`**
      * **`collectAsMap`**
      * `lookup`
* Caching
  * `MEMORY_ONLY`
  * `MEMORY_AND_DISK`
  * `MEMORY_ONLY_SER`
  * `MEMORY_AND_DISK_SER`
  * `DISK_ONLY`
* sparkSQL
  * DataFrame
  * Dataset
* accumulator
  * write-only
* broadcast
  * read-only

## Depedencies:

* Java 8
* Gradle
* IntelliJ

## How to Run

1. Execute `./gradlew idea` under this directory

2. Open with IntelliJ

## Data sets

Data sets are under `in` folder, including:

* airports.text
* nasa_19950701.tsv, nasa_19950801.tsv
* 2016-stack-overflow-survey-responses.csv
* RealEstate.csv
