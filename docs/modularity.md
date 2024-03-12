---
title: Doric Documentation
permalink: docs/modularity/
---


# Fostering modularity

Modular programming with Spark `Column` expressions is simply not possible. For instance, let's try to solve a simple
problem in a modular way. We start from the following `DataFrame`, whose columns finish with the same suffix "_user":


```scala
userDF.show()
// +---------+---------+--------+
// |name_user|city_user|age_user|
// +---------+---------+--------+
// |      Foo|   Madrid|      35|
// |      Bar| New York|      40|
// |     John|    Paris|      30|
// +---------+---------+--------+
// 
userDF.printSchema()
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
//and many more
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
// org.apache.spark.sql.AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `name1_user` cannot be resolved. Did you mean one of the following? [`name_user`, `age_user`, `city_user`].;
// 'Project ['name1_user]
// +- Project [_1#316 AS name_user#323, _2#317 AS city_user#324, _3#318 AS age_user#325]
//    +- LocalRelation [_1#316, _2#317, _3#318]
// 
// 	at org.apache.spark.sql.errors.QueryCompilationErrors$.unresolvedAttributeError(QueryCompilationErrors.scala:307)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.org$apache$spark$sql$catalyst$analysis$CheckAnalysis$$failUnresolvedAttribute(CheckAnalysis.scala:147)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis0$6(CheckAnalysis.scala:266)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis0$6$adapted(CheckAnalysis.scala:264)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:244)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis0$5(CheckAnalysis.scala:264)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis0$5$adapted(CheckAnalysis.scala:264)
// 	at scala.collection.immutable.Stream.foreach(Stream.scala:533)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis0$2(CheckAnalysis.scala:264)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis0$2$adapted(CheckAnalysis.scala:182)
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
//   [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `team_user` cannot be resolved. Did you mean one of the following? [`name_user`, `city_user`, `age_user`].
//   	located at . (modularity.md:79)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:50)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:140)
// 	at repl.MdocSession$MdocApp$$anonfun$2.apply(modularity.md:89)
// 	at repl.MdocSession$MdocApp$$anonfun$2.apply(modularity.md:86)
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
//   [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `team_user` cannot be resolved. Did you mean one of the following? [`name_user`, `city_user`, `age_user`].
//   	located at . (modularity.md:136)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:50)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:140)
// 	at repl.MdocSession$MdocApp5$$anonfun$3.apply(modularity.md:137)
// 	at repl.MdocSession$MdocApp5$$anonfun$3.apply(modularity.md:134)
```

