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
List(1,2,3).toDF.select(f.col("id")+1)
// org.apache.spark.sql.AnalysisException: cannot resolve 'id' given input columns: [value];
// 'Project [unresolvedalias(('id + 1), Some(org.apache.spark.sql.Column$$Lambda$4027/0x000000010174b040@28f1fc9d))]
// +- LocalRelation [value#292]
// 
// 	at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:54)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$$nestedInanonfun$checkAnalysis$1$2.applyOrElse(CheckAnalysis.scala:179)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$$nestedInanonfun$checkAnalysis$1$2.applyOrElse(CheckAnalysis.scala:175)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$2(TreeNode.scala:535)
// 	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:82)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:535)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:532)
// 	at org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1148)
// 	at org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1147)
// 	at org.apache.spark.sql.catalyst.expressions.BinaryExpression.mapChildren(Expression.scala:555)
```

```scala
// Doric
List(1,2,3).toDF.select(colInt("id")+1)
// doric.sem.DoricMultiError: Found 1 error in select
//   Cannot resolve column name "id" among (value)
//   	located at . (validations.md:37)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:139)
// 	at repl.MdocSession$App$$anonfun$2.apply(validations.md:37)
// 	at repl.MdocSession$App$$anonfun$2.apply(validations.md:37)
// Caused by: org.apache.spark.sql.AnalysisException: Cannot resolve column name "id" among (value)
// 	at org.apache.spark.sql.errors.QueryCompilationErrors$.cannotResolveColumnNameAmongFieldsError(QueryCompilationErrors.scala:2261)
// 	at org.apache.spark.sql.Dataset.org$apache$spark$sql$Dataset$$resolveException(Dataset.scala:258)
// 	at org.apache.spark.sql.Dataset.$anonfun$resolve$1(Dataset.scala:250)
// 	at scala.Option.getOrElse(Option.scala:189)
// 	at org.apache.spark.sql.Dataset.resolve(Dataset.scala:250)
// 	at org.apache.spark.sql.Dataset.col(Dataset.scala:1352)
// 	at org.apache.spark.sql.Dataset.apply(Dataset.scala:1319)
// 	at doric.types.SparkType.$anonfun$validate$1(SparkType.scala:56)
// 	at cats.data.KleisliApply.$anonfun$product$2(Kleisli.scala:674)
// 	at cats.data.Kleisli.$anonfun$map$1(Kleisli.scala:40)
```

## Mismatch types

But doric goes further, thanks to the type annotations that it supports. Indeed, let's assume that the column 
exists but its type is not what we expected: Spark won't be able to detect that, since type expectations are not 
encoded in plain columns. Thus, the following code will compile and execute without errors:

```scala
val df = List("1","2","three").toDF.select(f.col("value") + 1)
// df: org.apache.spark.sql.package.DataFrame = [(value + 1): double]
```

and we will be able to run the DataFrame:

```scala
df.show
// +-----------+
// |(value + 1)|
// +-----------+
// |        2.0|
// |        3.0|
// |       null|
// +-----------+
//
```

obtaining null values and garbage results, in general.

Using doric we can prevent the creation of the DataFrame, since column expressions are typed:

```scala
val df = List("1","2","three").toDF.select(colInt("value") + 1.lit)
// doric.sem.DoricMultiError: Found 1 error in select
//   The column with name 'value' is of type StringType and it was expected to be IntegerType
//   	located at . (validations.md:59)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:139)
// 	at repl.MdocSession$App$$anonfun$3.apply$mcV$sp(validations.md:59)
// 	at repl.MdocSession$App$$anonfun$3.apply(validations.md:58)
// 	at repl.MdocSession$App$$anonfun$3.apply(validations.md:58)
```

More on error reporting in our next [section](errors.md).