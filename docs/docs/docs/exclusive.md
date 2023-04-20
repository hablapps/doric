---
title: Doric Documentation
permalink: docs/doric-exclusive-features/
---

# Doric exclusive features

```scala mdoc:invisible
import org.apache.spark.sql.{functions => f, Row}

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
            x.getChild[Long]("col2") + 1L.lit // increasing in 1 the count of elements
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

## Custom sort for array columns & structured array columns
Maybe you had to sort arrays more than a couple of times, not a big deal, but maybe you had to sort arrays of structs... Now it gets interesting.

Currently, (spark 3.3.2 is the latest version) there is only one API function to sort arrays called `array_sort`, this will sort in descendant order any array type (in case of structs it will sort taking account the first column, then the second and so on). If you want to perform some custom order you have to write your own "spark function" or create an expression via SQL using which allows you to use the lambda function.

Doric provides this function for earlier versions (since spark 3.0). In fact, doric provides a simplified functions in case you need to order a structured array column just providing the sub-column names instead of creating an ad-hoc order function

```scala mdoc
case class Character(name: String, description: String, age: Int)
org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)

val dfArrayStruct = Seq(
  Seq(
    Character("Terminator", "T-800", 80),
    Character("Yoda", null, 900),
    Character("Gandalf", "white", 2),
    Character("Terminator", "T-1000", 1),
    Character("Gandalf", "grey", 2000)
  )
).toDF

val sparkCol = f.expr("array_sort(value, (l, r) -> case " +
  // name ASC
  "when l.name < r.name then -1 " +
  "when l.name > r.name then 1 " +
  "else ( case" +
  // age DESC
  "  when l.age > r.age then -1 " +
  "  when l.age < r.age then 1 " +
  "  else 0 end " +
  ") end)"
)

val doricCol = colArray[Row]("value").sortBy(CName("name"), CNameOrd("age", Desc))

dfArrayStruct.select(sparkCol.as("sorted")).show(false)
dfArrayStruct.select(doricCol.as("sorted")).show(false)

```
