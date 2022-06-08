---
title: Doric Documentation
permalink: docs/modularity/
---


# Fostering modularity

Modular programming with Spark `Column` expressions is simply not possible. For instance, let's try to solve a simple
problem in a modular way. We start from the following `DataFrame`, whose columns finish with the same suffix "_user":


```scala
userDF.show
// +---------+---------+--------+
// |name_user|city_user|age_user|
// +---------+---------+--------+
// |      Foo|   Madrid|      35|
// |      Bar| New York|      40|
// |     John|    Paris|      30|
// +---------+---------+--------+
// 
userDF.printSchema
// root
//  |-- name_user: string (nullable = true)
//  |-- city_user: string (nullable = true)
//  |-- age_user: integer (nullable = false)
//
```

We may refer to these columns using their full names: 

```scala
f.col("name_user")
// res2: Column = name_user
f.col("age_user")
// res3: Column = age_user
f.col("city_user")
// res4: Column = city_user
```

But we may also want to create a _reusable_ function which abstract away the common suffix and allows us to focus
on the unique part of the column names (thus obtaining a more modular solution):

```scala
def userCol(colName: String): Column =
  f.col(colName + "_user")
```

If we refer to an existing prefix, everything is fine. But, if we refer to a non-existing column in a `select` 
statement, for instance, then we would like to know where exactly that reference was made. The problem is that Spark 
will refer us to the line of the `select` statement instead: 

```scala
val userc = userCol("name1") // actual location of error :S
userDF.select(userc)        // error location reported by Spark
// org.apache.spark.sql.AnalysisException: cannot resolve 'name1_user' given input columns: [age_user, city_user, name_user];
// 'Project ['name1_user]
// +- Project [_1#208 AS name_user#215, _2#209 AS city_user#216, _3#210 AS age_user#217]
//    +- LocalRelation [_1#208, _2#209, _3#210]
// 
// 	at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:54)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$$nestedInanonfun$checkAnalysis$1$2.applyOrElse(CheckAnalysis.scala:179)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$$nestedInanonfun$checkAnalysis$1$2.applyOrElse(CheckAnalysis.scala:175)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$2(TreeNode.scala:535)
// 	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:82)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:535)
// 	at org.apache.spark.sql.catalyst.plans.QueryPlan.$anonfun$transformExpressionsUpWithPruning$1(QueryPlan.scala:181)
// 	at org.apache.spark.sql.catalyst.plans.QueryPlan.$anonfun$mapExpressions$1(QueryPlan.scala:193)
// 	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:82)
// 	at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpression$1(QueryPlan.scala:193)
```

### Towards the source of error

Doric includes in the exception the exact line of the malformed column reference (cf. [error reporting in doric](errors.md)),
and this will be of great help in solving our problem. However, the following doric function doesn't really work:

```scala
import doric.types.SparkType
def user[T: SparkType](colName: String): DoricColumn[T] = {
  col[T](colName + "_user")
}
```

Indeed, the following code will point out to the implementation of the `user` function: 

```scala
val age = user[Int]("name")
val team = user[String]("team")
userDF.select(age, team)
// doric.sem.DoricMultiError: Found 2 errors in select
//   The column with name 'name_user' was expected to be IntegerType but is of type StringType
//   	located at . (modularity.md:79)
//   Cannot resolve column name "team_user" among (name_user, city_user, age_user)
//   	located at . (modularity.md:79)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:139)
// 	at repl.MdocSession$App$$anonfun$2.apply(modularity.md:89)
// 	at repl.MdocSession$App$$anonfun$2.apply(modularity.md:86)
```


To get the right line of code in the reported error, we can just add the following implicit parameter to the `user`
function: 


```scala
import doric._
import doric.sem.Location
import doric.types.SparkType

def user[T: SparkType](colName: String)(implicit location: Location): DoricColumn[T] = {
  col[T](colName + "_user")
}
```

Now, if we repeat the same error we will be referred to the real problem:

```scala
val age = user[Int]("name")
val team = user[String]("team")
userDF.select(age, team)
// doric.sem.DoricMultiError: Found 2 errors in select
//   The column with name 'name_user' was expected to be IntegerType but is of type StringType
//   	located at . (modularity.md:135)
//   Cannot resolve column name "team_user" among (name_user, city_user, age_user)
//   	located at . (modularity.md:136)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:139)
// 	at repl.MdocSession$App5$$anonfun$3.apply(modularity.md:137)
// 	at repl.MdocSession$App5$$anonfun$3.apply(modularity.md:134)
```

