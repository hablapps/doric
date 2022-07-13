---
title: Doric Documentation
permalink: docs/
---


# Quick start

__Installation__

To use doric, just add the following dependency in your favourite build tool:

_Sbt_
```scala
libraryDependencies += "org.hablapps" % "doric_3-2_2.12" % "0.0.4"
```
_Maven_
```xml
<dependency>
  <groupId>org.hablapps</groupId>
  <artifactId>doric_3-2_2.12</artifactId>
  <version>0.0.4</version>
</dependency>
```

Doric is committed to use the most modern APIs first.
<!-- * Doric is compatible with Spark version 3.3.0. -->
* The latest stable version of doric is 0.0.4.
* The latest experimental version of doric is 0.0.0+1-3f901284-SNAPSHOT.
* Doric is compatible with the following Spark versions:

| Spark | Scala | Tested |                                                                            doric                                                                             |
|:-----:|:-----:|:------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------:|
| 2.4.1 | 2.11  |   ✅   |                                                                              -                                                                               |
| 2.4.2 | 2.11  |   ✅   |                                                                              -                                                                               |
| 2.4.3 | 2.11  |   ✅   |                                                                              -                                                                               |
| 2.4.4 | 2.11  |   ✅   |                                                                              -                                                                               |
| 2.4.5 | 2.11  |   ✅   |                                                                              -                                                                               |
| 2.4.6 | 2.11  |   ✅   |                                                                              -                                                                               |
| 2.4.7 | 2.11  |   ✅   |                                                                              -                                                                               |
| 2.4.8 | 2.11  |   ✅   | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_2-4_2.11)](https://mvnrepository.com/artifact/org.hablapps/doric_2-4_2.11/0.0.4) |
| 3.0.0 | 2.12  |   ✅   |                                                                              -                                                                               |
| 3.0.1 | 2.12  |   ✅   |                                                                              -                                                                               |
| 3.0.2 | 2.12  |   ✅   | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-0_2.12)](https://mvnrepository.com/artifact/org.hablapps/doric_3-0_2.12/0.0.4) |
| 3.1.0 | 2.12  |   ✅   |                                                                              -                                                                               |
| 3.1.1 | 2.12  |   ✅   |                                                                              -                                                                               |
| 3.1.2 | 2.12  |   ✅   | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-1_2.12)](https://mvnrepository.com/artifact/org.hablapps/doric_3-1_2.12/0.0.4) |
| 3.2.0 | 2.12  |   ✅   |                                                                              -                                                                               |
| 3.2.1 | 2.12  |   ✅   | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-2_2.12)](https://mvnrepository.com/artifact/org.hablapps/doric_3-2_2.12/0.0.4) |


__Import statements__

Doric is very easy to work with. First, you require the following import clause:

```scala
import doric._
```

There is no problem in combining conventional Spark column expressions and doric columns.
However, to avoid name clashes, we will use the prefix `f` for the former ones:

```scala
import org.apache.spark.sql.{functions => f}
``` 

## Type-safe column expressions

The overall purpose of doric is providing a type-safe API on top of the DataFrame API. This essentially means 
that we aim at capturing errors at compile time. For instance, in Spark we can't mix apples and oranges, but this 
code still compiles:
```scala
def df = List(1,2,3).toDF.select($"value" * f.lit(true))
```
It's only when we try to construct the DataFrame that an exception is raised at _run-time_:
```scala
df
// org.apache.spark.sql.AnalysisException: cannot resolve '(value * true)' due to data type mismatch: differing types in '(value * true)' (int and boolean).;
// 'Project [unresolvedalias((value#257 * true), Some(org.apache.spark.sql.Column$$Lambda$4158/0x0000000101845840@236d79d3))]
// +- LocalRelation [value#257]
// 
// 	at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis$7(CheckAnalysis.scala:212)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis$7$adapted(CheckAnalysis.scala:192)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:367)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$foreachUp$1(TreeNode.scala:366)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$foreachUp$1$adapted(TreeNode.scala:366)
// 	at scala.collection.Iterator.foreach(Iterator.scala:943)
// 	at scala.collection.Iterator.foreach$(Iterator.scala:943)
// 	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
// 	at scala.collection.IterableLike.foreach(IterableLike.scala:74)
```

Using doric, there is no need to wait for so long: errors will be reported at compile-time!
```scala
List(1,2,3).toDF.select(col[Int]("value") * lit(true))
// error: type mismatch;
//  found   : Boolean(true)
//  required: Int
// List(1,2,3).toDF.select(col[Int]("value") * lit(true))
//                                                 ^^^^
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

```scala
List(1,2,3).toDF.filter(col[Int]("value") > lit(1))
// res1: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [value: int]
```

As you can see in [validations](validations.md), explicit type annotations enable further validations when columns
are interpreted within the context of `withColumn`, `select`, etc.

## Mixing doric and Spark columns

Since doric is intended as a replacement of the _whole_ DataFrame API, type-safe versions of Spark functions 
for numbers, dates, strings, etc., are provided. To know all possible transformations, you can take a look at 
the [DoricColumn API](https://www.hablapps.com/doric/docs/api/3.2/scala-2.12/api/doric/DoricColumn.html) .Occasionally, however, we might need to mix 
both doric and Spark column expressions. There is no problem with that, as this example shows: 

```scala
val strDf = List("hi", "welcome", "to", "doric").toDF("str")
// strDf: org.apache.spark.sql.package.DataFrame = [str: string]

strDf
  .select(f.concat(f.col("str"), f.lit("!!!")) as "newCol") //pure spark
  .select(concat(lit("???"), colString("newCol")) as "finalCol") //pure and sweet doric
  .show()
// +-------------+
// |     finalCol|
// +-------------+
// |     ???hi!!!|
// |???welcome!!!|
// |     ???to!!!|
// |  ???doric!!!|
// +-------------+
//
```

Also, we can transform pure Spark columns into doric columns, and be sure that specific doric [validations](validations.md)
will be applied:
```scala
strDf.select(f.col("str").asDoric[String]).show()
// +-------+
// |    str|
// +-------+
// |     hi|
// |welcome|
// |     to|
// |  doric|
// +-------+
//
```

```scala

strDf.select((f.col("str") + f.lit(true)).asDoric[String]).show
// doric.sem.DoricMultiError: Found 1 error in select
//   cannot resolve '(CAST(str AS DOUBLE) + true)' due to data type mismatch: differing types in '(CAST(str AS DOUBLE) + true)' (double and boolean).;
//   'Project [unresolvedalias((cast(str#270 as double) + true), Some(org.apache.spark.sql.Column$$Lambda$4158/0x0000000101845840@236d79d3))]
//   +- Project [value#267 AS str#270]
//      +- LocalRelation [value#267]
//   
//   	located at . (quickstart.md:76)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:139)
// 	at repl.MdocSession$App$$anonfun$2.apply$mcV$sp(quickstart.md:76)
// 	at repl.MdocSession$App$$anonfun$2.apply(quickstart.md:76)
// 	at repl.MdocSession$App$$anonfun$2.apply(quickstart.md:76)
// Caused by: org.apache.spark.sql.AnalysisException: cannot resolve '(CAST(str AS DOUBLE) + true)' due to data type mismatch: differing types in '(CAST(str AS DOUBLE) + true)' (double and boolean).;
// 'Project [unresolvedalias((cast(str#270 as double) + true), Some(org.apache.spark.sql.Column$$Lambda$4158/0x0000000101845840@236d79d3))]
// +- Project [value#267 AS str#270]
//    +- LocalRelation [value#267]
// 
// 	at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis$7(CheckAnalysis.scala:212)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis$7$adapted(CheckAnalysis.scala:192)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:367)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$foreachUp$1(TreeNode.scala:366)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$foreachUp$1$adapted(TreeNode.scala:366)
// 	at scala.collection.Iterator.foreach(Iterator.scala:943)
// 	at scala.collection.Iterator.foreach$(Iterator.scala:943)
// 	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
// 	at scala.collection.IterableLike.foreach(IterableLike.scala:74)
```
