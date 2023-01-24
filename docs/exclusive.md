---
title: Doric Documentation
permalink: docs/doric-exclusive-featues/
---


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

```scala
spark.range(10).show()
// +---+
// | id|
// +---+
// |  0|
// |  1|
// |  2|
// |  3|
// |  4|
// |  5|
// |  6|
// |  7|
// |  8|
// |  9|
// +---+
// 

spark.range(10).select(customMean.as("customMean")).show()
// +----------+
// |customMean|
// +----------+
// |       4.5|
// +----------+
//
```

