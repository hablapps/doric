---
title: Doric Documentation
permalink: docs/validations/
---


# Doric validations

Doric is a type-safe API, which means that many common errors will be captured at compile-time. However, there
are errors which can't be anticipated, since they depend on the actual datasource available at runtime. For instance, 
we might make reference to a non-existing column. In this case, doric behaves similarly to Spark,
raising a run-time exception: 

```scala
// Spark
List(1,2,3).toDF().select(f.col("id")+1)
// org.apache.spark.sql.AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `id` cannot be resolved. Did you mean one of the following? [`value`].;
// 'Project [unresolvedalias(('id + 1), Some(org.apache.spark.sql.Column$$Lambda$3790/0x00000008016c6840@115ac121))]
// +- LocalRelation [value#399]
// 
// 	at org.apache.spark.sql.errors.QueryCompilationErrors$.unresolvedAttributeError(QueryCompilationErrors.scala:306)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.org$apache$spark$sql$catalyst$analysis$CheckAnalysis$$failUnresolvedAttribute(CheckAnalysis.scala:141)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis0$6(CheckAnalysis.scala:299)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis0$6$adapted(CheckAnalysis.scala:297)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:244)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$foreachUp$1(TreeNode.scala:243)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$foreachUp$1$adapted(TreeNode.scala:243)
// 	at scala.collection.Iterator.foreach(Iterator.scala:943)
// 	at scala.collection.Iterator.foreach$(Iterator.scala:943)
// 	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
```

```scala
// Doric
List(1,2,3).toDF().select(colInt("id")+1)
// doric.sem.DoricMultiError: Found 1 error in select
//   [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `id` cannot be resolved. Did you mean one of the following? [`value`].
//   	located at . (validations.md:37)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:50)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:140)
// 	at repl.MdocSession$MdocApp$$anonfun$2.apply(validations.md:37)
// 	at repl.MdocSession$MdocApp$$anonfun$2.apply(validations.md:37)
// Caused by: org.apache.spark.sql.AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `id` cannot be resolved. Did you mean one of the following? [`value`].
// 	at org.apache.spark.sql.errors.QueryCompilationErrors$.unresolvedColumnWithSuggestionError(QueryCompilationErrors.scala:3109)
// 	at org.apache.spark.sql.errors.QueryCompilationErrors$.resolveException(QueryCompilationErrors.scala:3117)
// 	at org.apache.spark.sql.Dataset.$anonfun$resolve$1(Dataset.scala:251)
// 	at scala.Option.getOrElse(Option.scala:189)
// 	at org.apache.spark.sql.Dataset.resolve(Dataset.scala:251)
// 	at org.apache.spark.sql.Dataset.col(Dataset.scala:1428)
// 	at org.apache.spark.sql.Dataset.apply(Dataset.scala:1395)
// 	at doric.types.SparkType.$anonfun$validate$1(SparkType.scala:61)
// 	at cats.data.KleisliApply.$anonfun$product$2(Kleisli.scala:735)
// 	at scala.Function1.$anonfun$andThen$1(Function1.scala:57)
```

## Mismatch types

But doric goes further, thanks to the type annotations that it supports. Indeed, let's assume that the column 
exists but its type is not what we expected: Spark won't be able to detect that, since type expectations are not 
encoded in plain columns. Thus, the following code will compile and execute without errors:

```scala
val df = List("1","2","three").toDF().select(f.col("value") + 1)
// df: org.apache.spark.sql.package.DataFrame = [(value + 1): double]
```

and we will be able to run the DataFrame:

```scala
df.show()
// +-----------+
// |(value + 1)|
// +-----------+
// |        2.0|
// |        3.0|
// |       NULL|
// +-----------+
//
```

obtaining null values and garbage results, in general.

Using doric we can prevent the creation of the DataFrame, since column expressions are typed:

```scala
val df = List("1","2","three").toDF().select(colInt("value") + 1.lit)
// doric.sem.DoricMultiError: Found 1 error in select
//   The column with name 'value' was expected to be IntegerType but is of type StringType
//   	located at . (validations.md:59)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:50)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:140)
// 	at repl.MdocSession$MdocApp$$anonfun$3.apply$mcV$sp(validations.md:59)
// 	at repl.MdocSession$MdocApp$$anonfun$3.apply(validations.md:58)
// 	at repl.MdocSession$MdocApp$$anonfun$3.apply(validations.md:58)
```

More on error reporting in our next [section](errors.md).