---
title: Doric Documentation
permalink: docs/errors/
---

```scala mdoc:invisible
import org.apache.spark.sql.{functions => f}

val spark = doric.DocInit.getSpark
import spark.implicits._

import doric._
import doric.implicitConversions._
```

# Doric error management

## Error location

Let's see again the error raised by doric when making a reference to a non-existing column:
```scala mdoc:crash
// Doric
List(1,2,3).toDF().select(colInt("id")+1)
```

As you may have already noticed, there is a slight difference with the exception reported by Spark: doric adds precise 
information about the location of the error in the source code, which in many cases is immensely useful (e.g. to 
support the development of [reusable functions](modularity.md)). 

## Error aggregation

Doric departs from Spark in an additional aspect of error management: Sparks adopts a fail-fast strategy, in such 
a way that it will stop at the first error encountered, whereas doric will keep accumulating errors throughout the
whole column expression. This is essential to speed up and facilitate the solution to most common development problems.

For instance, let's consider the following code where we encounter three erroneous column references:

```scala mdoc:silent
val dfPair = List(("hi", 31)).toDF("str", "int")
val col1 = colInt("str")   // existing column, wrong type
val col2 = colString("int") // existing column, wrong type
val col3 = colInt("unknown") // non-existing column
```

```scala mdoc:crash
dfPair.select(col1, col2, col3)
```

As we can see, the select expression throws a _single_ exception reporting the three different errors. There is no
need to start an annoying fix-rerun loop until all errors are found. Moreover, note that each error points to the 
line of the corresponding column expression where it took place. 

