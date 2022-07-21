---
title: Doric Documentation
permalink: docs/validations/
---

```scala mdoc:invisible
import org.apache.spark.sql.{functions => f}

val spark = doric.DocInit.getSpark
import spark.implicits._

import doric._
import doric.implicitConversions._
```

# Doric validations

Doric is a type-safe API, which means that many common errors will be captured at compile-time. However, there
are errors which can't be anticipated, since they depend on the actual datasource available at runtime. For instance, 
we might make reference to a non-existing column. In this case, doric behaves similarly to Spark,
raising a run-time exception: 

```scala mdoc:crash
// Spark
List(1,2,3).toDF().select(f.col("id")+1)
```

```scala mdoc:crash
// Doric
List(1,2,3).toDF().select(colInt("id")+1)
```

## Mismatch types

But doric goes further, thanks to the type annotations that it supports. Indeed, let's assume that the column 
exists but its type is not what we expected: Spark won't be able to detect that, since type expectations are not 
encoded in plain columns. Thus, the following code will compile and execute without errors:

```scala mdoc
val df = List("1","2","three").toDF().select(f.col("value") + 1)
```

and we will be able to run the DataFrame:

```scala mdoc
df.show()
```

obtaining null values and garbage results, in general.

Using doric we can prevent the creation of the DataFrame, since column expressions are typed:

```scala mdoc:crash
val df = List("1","2","three").toDF().select(colInt("value") + 1.lit)
```

More on error reporting in our next [section](errors.md).