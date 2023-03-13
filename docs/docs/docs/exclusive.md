---
title: Doric Documentation
permalink: docs/doric-exclusive-featues/
---

```scala mdoc:invisible
import org.apache.spark.sql.{functions => f}

val spark = doric.DocInit.getSpark
import spark.implicits._

import doric._
import doric.implicitConversions._
import doric.AggExample.customMean
```

## Aggregations with doric syntax

Doric introduces a simpler way to implement user defined aggregations, using doric's own syntax.
The creation needs only five elements:

- The column to make the aggregation
- zero: the initialization of the value on each task.
- Update: the function to update the accumulated value when processing a new column value.
- Merge: a function to compare the results of all the Update results, two by two
- The final transformation of the value once all merges are applied.

The following example shows how to implement the average of a column of type int

```scala
      val customMean = customAgg[Long, Row, Double](
  col[Long]("id"), // The column to work on
  struct(lit(0L), lit(0L)), // setting the sum and the count to 0
  (x, y) =>
    struct(
      x.getChild[Long]("col1") + y, // adding the value to the sum
      x.getChild[Long]("col2") + 1L.lit // increasing in 1 the count of elemnts
    ),
  (x, y) =>
    struct(
      x.getChild[Long]("col1") + y.getChild[Long]("col1"), // obtaining the total sum of all 
      x.getChild[Long]("col2") + y.getChild[Long]("col2") // obtaining the total count of all
    ),
  x => x.getChild[Long]("col1") / x.getChild[Long]("col2") // the total sum divided by the count
)
```

Now you can use your new aggregation as usual

```scala mdoc
spark.range(10).show()

spark.range(10).select(customMean.as("customMean")).show()
```

## Column mappings/matches

Sometimes we must perform a mapping transformation based on a column value, so if its value is `key1` the output must
be `result1`, if the value is `key2` the output must be `result2`, and so on.

This is usually achieved by a `when` series using **spark**.

```scala mdoc
val dfMatch = Seq("key1", "key2", "key3", "anotherKey1", "anotherKey2").toDF()

val mapColSpark = f.when(f.col("value") === "key1", "result1")
  .when(f.col("value") === "key2", "result2") // actually we could write here a different column name, so the when will not work properly
  .when(f.length(f.col("value")) > 4, "error key")
  .otherwise(null)
```

We haven't reinvented the wheel, but now it is fail-proof (as we always match to the same column) and much simpler to
map values using **doric**:

```scala mdoc
val mapColDoric = colString("value").matches[String]
  // simple mappings, it is the same as if we use _ === "whatever"
  .caseW("key1".lit, "result1".lit)
  .caseW("key2".lit, "result2".lit)
  // function equality
  .caseW(_.length > 4, "error key".lit)
  .otherwiseNull

dfMatch.withColumn("mapResult", mapColDoric).show()
```

It is also a lot easier if you have a list of transformations, as we use the doric when builder under the hoods:
```scala mdoc:silent
val transformations = Map(
  "key1" -> "result1",
  "key2" -> "result2",
  "key4" -> "result4"
)

// spark
val sparkFold = transformations.tail.foldLeft(f.when(f.col("value") === transformations.head._1, transformations.head._2)) {
  case (cases, (key, value)) =>
    cases.when(f.col("value") === key, value)   // once again, what if I make a mistake and I write a different column?
}
  
sparkFold.otherwise(null)

// doric
val doricFold = transformations.foldLeft(colString("value").matches[String]) {
  case (cases, (key, value)) =>
    cases.caseW(key.lit, value.lit)
}
  
doricFold.otherwiseNull
```
