---
title: Actions
permalink: docs/actions/
---
# Actions
Actions are methods that will trigger a spark job.
This are marked to remark this behaviour.
## Validations
Validations allows to throw an error if the rows don't follow a validation,
capturing it as a normal doric error. In case that it passes the validation
it will return the column to continue the transformations
```scala mdoc:invisible
import org.apache.spark.sql.{SparkSession, DataFrame}

val spark = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .appName("spark session")
      .getOrCreate()
      
import spark.implicits._
```
For example, `noneNull` will check that none of the rows contains a null value to continue.
```scala mdoc
import doric._
val dfValid = List(1,2,3).toDF("id")
dfValid.select(colInt("id").action.validate.noneNull)
```
```scala mdoc:crash
val dfWithNull = List(Some(1), Some(2), None).toDF("id")
dfWithNull.select(colInt("id").action.validate.noneNull)
```
Also, provides all, a more open function to validate your data.
```scala mdoc
val dfNumbers = List(1,2,3).toDF("id")
dfNumbers.select(colInt("id").action.validate.all(_ > 0.lit))
```
```scala mdoc:crash
dfNumbers.select(colInt("id").action.validate.all(_ > 1.lit))
```
And allows to run in a single action multiple validations that must all pass.

```scala mdoc:crash
dfNumbers.select(colInt("id").action.validate.all(
  _ > 1.lit,
  _.isNotNull,
  _ < 3.lit
))
```
