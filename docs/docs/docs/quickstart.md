---
title: Doric Documentation
permalink: docs/
---


# Quick start

__Installation__

To use doric, just add the following dependency in your favourite build tool:

[![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_@SPARK_SHORT_VERSION@_@SCALA_SHORT_VERSION@)](https://mvnrepository.com/artifact/org.hablapps/doric_@SPARK_SHORT_VERSION@_@SCALA_SHORT_VERSION@/@STABLE_VERSION@)

_Sbt_
```scala
libraryDependencies += "org.hablapps" % "doric_@SPARK_SHORT_VERSION@_@SCALA_SHORT_VERSION@" % "@STABLE_VERSION@"
```
_Maven_
```xml
<dependency>
  <groupId>org.hablapps</groupId>
  <artifactId>doric_@SPARK_SHORT_VERSION@_@SCALA_SHORT_VERSION@</artifactId>
  <version>@STABLE_VERSION@</version>
</dependency>
```

Doric is committed to use the most modern APIs first.
<!-- * Doric is compatible with Spark version @SPARK_VERSION@. -->
* The latest stable version of doric is @STABLE_VERSION@.
* The latest experimental version of doric is @VERSION@.
* Doric is compatible with the following Spark versions:

| Spark | Scala | Tested |                                                                                  doric                                                                                  |
|:-----:|:-----:|:------:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
| 2.4.1 | 2.11  |   ✅    |                                                                                    -                                                                                    |
| 2.4.2 | 2.11  |   ✅    |                                                                                    -                                                                                    |
| 2.4.3 | 2.11  |   ✅    |                                                                                    -                                                                                    |
| 2.4.4 | 2.11  |   ✅    |                                                                                    -                                                                                    |
| 2.4.5 | 2.11  |   ✅    |                                                                                    -                                                                                    |
| 2.4.6 | 2.11  |   ✅    |                                                                                    -                                                                                    |
| 2.4.7 | 2.11  |   ✅    |                                                                                    -                                                                                    |
| 2.4.8 | 2.11  |   ✅    | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_2-4_2.11)](https://mvnrepository.com/artifact/org.hablapps/doric_2-4_2.11/@STABLE_VERSION@) |
| 3.0.0 | 2.12  |   ✅    |                                                                                    -                                                                                    |
| 3.0.1 | 2.12  |   ✅    |                                                                                    -                                                                                    |
| 3.0.2 | 2.12  |   ✅    | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-0_2.12)](https://mvnrepository.com/artifact/org.hablapps/doric_3-0_2.12/@STABLE_VERSION@) |
| 3.1.0 | 2.12  |   ✅    |                                                                                    -                                                                                    |
| 3.1.1 | 2.12  |   ✅    |                                                                                    -                                                                                    |
| 3.1.2 | 2.12  |   ✅    | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-1_2.12)](https://mvnrepository.com/artifact/org.hablapps/doric_3-1_2.12/@STABLE_VERSION@) |
| 3.2.0 | 2.12  |   ✅    |                                                                                    -                                                                                    |
| 3.2.0 | 2.13  |   ✅    |                                                                                    -                                                                                    |
| 3.2.1 | 2.12  |   ✅    |                                                                                    -                                                                                    |
| 3.2.1 | 2.13  |   ✅    |                                                                                    -                                                                                    |
| 3.2.2 | 2.12  |   ✅    |                                                                                    -                                                                                    |
| 3.2.2 | 2.13  |   ✅    |                                                                                    -                                                                                    |
| 3.2.3 | 2.12  |   ✅    | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-2_2.12)](https://mvnrepository.com/artifact/org.hablapps/doric_3-2_2.12/@STABLE_VERSION@) |
| 3.2.3 | 2.13  |   ✅    | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-2_2.13)](https://mvnrepository.com/artifact/org.hablapps/doric_3-2_2.13/@STABLE_VERSION@) |
| 3.3.0 | 2.12  |   ✅    |                                                                                    -                                                                                    |
| 3.3.0 | 2.13  |   ✅    |                                                                                    -                                                                                    |
| 3.3.1 | 2.12  |   ✅    |                                                                                    -                                                                                    |
| 3.3.1 | 2.13  |   ✅    |                                                                                    -                                                                                    |
| 3.3.2 | 2.12  |   ✅    | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-3_2.12)](https://mvnrepository.com/artifact/org.hablapps/doric_3-3_2.12/@STABLE_VERSION@) |
| 3.3.2 | 2.13  |   ✅    | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-3_2.13)](https://mvnrepository.com/artifact/org.hablapps/doric_3-3_2.13/@STABLE_VERSION@) |


__Import statements__

Doric is very easy to work with. First, you require the following import clause:

```scala mdoc
import doric._
```

There is no problem in combining conventional Spark column expressions and doric columns.
However, to avoid name clashes, we will use the prefix `f` for the former ones:

```scala mdoc
import org.apache.spark.sql.{functions => f}
``` 
```scala mdoc:invisible
val spark = doric.DocInit.getSpark
import spark.implicits._
```

## Type-safe column expressions

The overall purpose of doric is providing a type-safe API on top of the DataFrame API. This essentially means 
that we aim at capturing errors at compile time. For instance, in Spark we can't mix apples and oranges, but this 
code still compiles:
```scala mdoc
def df = List(1,2,3).toDF().select($"value" * f.lit(true))
```
It's only when we try to construct the DataFrame that an exception is raised at _run-time_:
```scala mdoc:crash
df
``` 

Using doric, there is no need to wait for so long: errors will be reported at compile-time!
```scala mdoc:fail
List(1,2,3).toDF().select(col[Int]("value") * lit(true))
```

As you may see, changes in column expressions are minimal: just annotate column references with the intended type, 
i.e. `col[Int]("name")`, instead of `col("name")`. With this extra bit of type information, we are not only
referring to a column named `name`: we are signalling that the expected Spark data type of that column is `Integer`. 

---
ℹ️ **NOTE** ℹ️

> Of course, this only works if we know the intended type
of the column at compile-time. In a pure dynamic setting, doric is useless. Note, however, that you don't need to know
in advance the whole row type, as with Datasets. Thus, doric sits between a wholehearted static setting and a
purely dynamic one. It offers type-safety for column expressions at a minimum cost, without compromising performance,
i.e. sticking to DataFrames.

---

Finally, once we have constructed a doric column expression, we can use it within the context of a `withColumn` expression,
or, in general, wherever we may use plain Spark columns: joins, filters, etc.:

```scala mdoc
List(1,2,3).toDF().filter(col[Int]("value") > lit(1))
```

As you can see in [validations](validations.md), explicit type annotations enable further validations when columns
are interpreted within the context of `withColumn`, `select`, etc.

## Mixing doric and Spark columns

Since doric is intended as a replacement of the _whole_ DataFrame API, type-safe versions of Spark functions 
for numbers, dates, strings, etc., are provided. To know all possible transformations, you can take a look at 
the [DoricColumn API](https://www.hablapps.com/doric/docs/api/spark-3.2/scala-2.12/doric/DoricColumn.html) .Occasionally, however, we might need to mix 
both doric and Spark column expressions. There is no problem with that, as this example shows: 

```scala mdoc
val strDf = List("hi", "welcome", "to", "doric").toDF("str")

strDf
  .select(f.concat(f.col("str"), f.lit("!!!")) as "newCol") //pure spark
  .select(concat(lit("???"), colString("newCol")) as "finalCol") //pure and sweet doric
  .show()
```

Also, we can transform pure Spark columns into doric columns, and be sure that specific doric [validations](validations.md)
will be applied:
```scala mdoc
strDf.select(f.col("str").asDoric[String]).show()
```

```scala mdoc:crash

strDf.select((f.col("str") + f.lit(true)).asDoric[String]).show()
```
