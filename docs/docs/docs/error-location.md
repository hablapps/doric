---
title: Pingpointing the error place
permalink: docs/errors
---

# Errors in doric
Doric is a typesafe APO, this means that the compiler will prevent to do transformations that will throw exceptions in runtime.
The only possible source of errors are the selection of columns, if the column doesn't exist or if the column contains an unexpected type.

```scala mdoc:invisible
import org.apache.spark.sql.{SparkSession, DataFrame}

val spark = SparkSession
      .builder()
      .master("local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .appName("spark session")
      .getOrCreate()
      
import spark.implicits._
```
Let's see an example of an error
```scala mdoc
import doric._

val df = List(("hi", 31)).toDF("str", "int")
val col1 = colInt("str")
val col2 = colString("int")
val col3 = colInt("unknown")
```
```scala mdoc:crash
df.select(col1, col2, col3)
```

