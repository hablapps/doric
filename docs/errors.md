---
title: Doric Documentation
permalink: docs/errors/
---


# Doric error management

## Error location

Let's see again the error raised by doric when making a reference to a non-existing column:
```scala
// Doric
List(1,2,3).toDF().select(colInt("id")+1)
// doric.sem.DoricMultiError: Found 1 error in select
//   Cannot resolve column name "id" among (value)
//   	located at . (errors.md:27)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:139)
// 	at repl.MdocSession$MdocApp$$anonfun$1.apply(errors.md:27)
// 	at repl.MdocSession$MdocApp$$anonfun$1.apply(errors.md:27)
// Caused by: org.apache.spark.sql.AnalysisException: Cannot resolve column name "id" among (value)
// 	at org.apache.spark.sql.errors.QueryCompilationErrors$.cannotResolveColumnNameAmongFieldsError(QueryCompilationErrors.scala:2264)
// 	at org.apache.spark.sql.Dataset.org$apache$spark$sql$Dataset$$resolveException(Dataset.scala:259)
// 	at org.apache.spark.sql.Dataset.$anonfun$resolve$1(Dataset.scala:251)
// 	at scala.Option.getOrElse(Option.scala:189)
// 	at org.apache.spark.sql.Dataset.resolve(Dataset.scala:251)
// 	at org.apache.spark.sql.Dataset.col(Dataset.scala:1417)
// 	at org.apache.spark.sql.Dataset.apply(Dataset.scala:1384)
// 	at doric.types.SparkType.$anonfun$validate$1(SparkType.scala:61)
// 	at cats.data.KleisliApply.$anonfun$product$2(Kleisli.scala:674)
// 	at cats.data.Kleisli.$anonfun$map$1(Kleisli.scala:40)
```

As you may have already noticed, there is a slight difference with the exception reported by Spark: doric adds precise 
information about the location of the error in the source code, which in many cases is immensely useful (e.g. to 
support the development of [reusable functions](modularity.md)). 

## Error aggregation

Doric departs from Spark in an additional aspect of error management: Sparks adopts a fail-fast strategy, in such 
a way that it will stop at the first error encountered, whereas doric will keep accumulating errors throughout the
whole column expression. This is essential to speed up and facilitate the solution to most common development problems.

For instance, let's consider the following code where we encounter three erroneous column references:

```scala
val dfPair = List(("hi", 31)).toDF("str", "int")
val col1 = colInt("str")   // existing column, wrong type
val col2 = colString("int") // existing column, wrong type
val col3 = colInt("unknown") // non-existing column
```

```scala
dfPair.select(col1, col2, col3)
// doric.sem.DoricMultiError: Found 3 errors in select
//   Cannot resolve column name "unknown" among (str, int)
//   	located at . (errors.md:45)
//   The column with name 'int' was expected to be StringType but is of type IntegerType
//   	located at . (errors.md:42)
//   The column with name 'str' was expected to be IntegerType but is of type StringType
//   	located at . (errors.md:39)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:139)
// 	at repl.MdocSession$MdocApp$$anonfun$2.apply(errors.md:52)
// 	at repl.MdocSession$MdocApp$$anonfun$2.apply(errors.md:52)
```

As we can see, the select expression throws a _single_ exception reporting the three different errors. There is no
need to start an annoying fix-rerun loop until all errors are found. Moreover, note that each error points to the 
line of the corresponding column expression where it took place. 

