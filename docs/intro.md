---
title: Doric Documentation
permalink: docs/
---

# Introduction to Doric
## First steps with Doric

Doric is very easy to work with, follow the [installation guide]({{ site.baseurl }}{% link docs/installation.md %})
first.

Then just import doric to your code


```scala
import doric._
```

Doric adds an extra layer of knowledge to your column assigning a type, that is checked against the spark datatype in the
dataframe. As in the case of a dataframe that we ask the wrong column name, doric will also detect that the type is not
the expected.
```scala
import spark.implicits._

val df = List("hi", "welcome", "to", "doric").toDF("str")
// df: org.apache.spark.sql.package.DataFrame = [str: string]

df.select(col[String]("str")).show()
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
val wrongName = col[String]("string")
df.select(wrongName)
// doric.sem.DoricMultiError: Found 1 error in select
//   Cannot resolve column name "string" among (str)
//   	located at . (intro.md:33)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:139)
// 	at repl.MdocSession$App$$anonfun$4.apply(intro.md:34)
// 	at repl.MdocSession$App$$anonfun$4.apply(intro.md:32)
// Caused by: org.apache.spark.sql.AnalysisException: Cannot resolve column name "string" among (str)
// 	at org.apache.spark.sql.Dataset.org$apache$spark$sql$Dataset$$resolveException(Dataset.scala:272)
// 	at org.apache.spark.sql.Dataset.$anonfun$resolve$1(Dataset.scala:263)
// 	at scala.Option.getOrElse(Option.scala:189)
// 	at org.apache.spark.sql.Dataset.resolve(Dataset.scala:263)
// 	at org.apache.spark.sql.Dataset.col(Dataset.scala:1359)
// 	at org.apache.spark.sql.Dataset.apply(Dataset.scala:1326)
// 	at doric.types.SparkType.$anonfun$validate$1(SparkType.scala:56)
// 	at cats.data.Kleisli.$anonfun$map$1(Kleisli.scala:40)
// 	at cats.data.Kleisli.$anonfun$map$1(Kleisli.scala:40)
// 	at cats.data.Kleisli.$anonfun$map$1(Kleisli.scala:40)
```

```scala
val wrongType = col[Int]("str")
df.select(wrongType)
// doric.sem.DoricMultiError: Found 1 error in select
//   The column with name 'str' is of type StringType and it was expected to be IntegerType
//   	located at . (intro.md:44)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:139)
// 	at repl.MdocSession$App$$anonfun$6.apply(intro.md:45)
// 	at repl.MdocSession$App$$anonfun$6.apply(intro.md:43)
```
This type of errors are obtained in runtime, but now that we know the exact type of the column,
we can operate according to the type.

```scala
val concatCol = concat(col[String]("str"), col("str"))
// concatCol: StringColumn = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$1475/2107284848@2c9ecb60)
// )
df.select(concatCol).show()
// +----------------+
// |concat(str, str)|
// +----------------+
// |            hihi|
// |  welcomewelcome|
// |            toto|
// |      doricdoric|
// +----------------+
//
```

And won't allow to do any operation that is not logic in compile time.
```scala
val stringPlusInt = col[Int]("int") + col[String]("str")
// error: type mismatch;
//  found   : doric.NamedDoricColumn[String]
//  required: doric.DoricColumn[Int]
// val stringPlusInt = col[Int]("int") + col[String]("str")
//                                                  ^
```
 This way we won't have any kind of unexpected behaviour in our process.
 
## Mix doric and spark
Doric only adds method to your everyday Spark Dataframe, you can mix spark selects and doric.

```scala
import org.apache.spark.sql.{functions => f}
df
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

Also, if you don't want to use doric to transform a column, you can transform a pure spark column into doric, and be sure that the type of the transformation is ok.
```scala
df.select(f.col("str").asDoric[String]).show()
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

But we recommend to use always the columns selectors from doric to prevent errors that doric can detect in compile time
```scala
val sparkToDoricColumn = (f.col("str") + f.lit(true)).asDoric[String]
df.select(sparkToDoricColumn).show
// doric.sem.DoricMultiError: Found 1 error in select
//   cannot resolve '(CAST(`str` AS DOUBLE) + true)' due to data type mismatch: differing types in '(CAST(`str` AS DOUBLE) + true)' (double and boolean).;
//   'Project [(cast(str#247 as double) + true) AS (str + true)#285]
//   +- Project [value#244 AS str#247]
//      +- LocalRelation [value#244]
//   
//   	located at . (intro.md:88)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:139)
// 	at repl.MdocSession$App$$anonfun$13.apply$mcV$sp(intro.md:89)
// 	at repl.MdocSession$App$$anonfun$13.apply(intro.md:87)
// 	at repl.MdocSession$App$$anonfun$13.apply(intro.md:87)
// Caused by: org.apache.spark.sql.AnalysisException: cannot resolve '(CAST(`str` AS DOUBLE) + true)' due to data type mismatch: differing types in '(CAST(`str` AS DOUBLE) + true)' (double and boolean).;
// 'Project [(cast(str#247 as double) + true) AS (str + true)#285]
// +- Project [value#244 AS str#247]
//    +- LocalRelation [value#244]
// 
// 	at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$$nestedInanonfun$checkAnalysis$1$2.applyOrElse(CheckAnalysis.scala:161)
// 	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$$nestedInanonfun$checkAnalysis$1$2.applyOrElse(CheckAnalysis.scala:152)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUp$2(TreeNode.scala:342)
// 	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:74)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:342)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUp$1(TreeNode.scala:339)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$mapChildren$1(TreeNode.scala:408)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:244)
// 	at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:406)
```

In spark the sum of a string with a boolean will throw an error in runtime. In doric this code won't be able to compile.
```scala
col[String]("str") + true.lit
// error: type mismatch;
//  found   : doric.LiteralDoricColumn[Boolean]
//  required: doric.StringColumn
//     (which expands to)  doric.DoricColumn[String]
// col[String]("str") + true.lit
//                      ^^^^^^^^
```

## Sweet doric syntax sugar
### Column selector alias
We know that doric can be seen as an extra boilerplate to get the columns, that's why we provide some extra methods to acquire the columns.

```scala
colString("str") // similar to col[String]("str")
// res6: NamedDoricColumn[String] = NamedDoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$1471/1605170188@440404f5),
//   "str"
// ) // similar to col[String]("str")
colInt("int") // similar to col[Int]("int")
// res7: NamedDoricColumn[Int] = NamedDoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$1471/1605170188@51fcb8d0),
//   "int"
// ) // similar to col[Int]("int")
colArray[Int]("int") // similar to col[Array[Int]]("int")
// res8: NamedDoricColumn[Array[Int]] = NamedDoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$1471/1605170188@1df73457),
//   "int"
// )
```
### Readable syntax
Doric tries to be less SQL verbose, and adopt a more object-oriented API, allowing the developer to view with the dot notation of scala the methods that can be used.
```scala
val dfArrays = List(("string", Array(1,2,3))).toDF("str", "arr")
// dfArrays: org.apache.spark.sql.package.DataFrame = [str: string, arr: array<int>]
```
Spark SQL method-argument way to develop
```scala
import org.apache.spark.sql.Column

val sArrCol: Column = f.col("arr")
// sArrCol: Column = arr
val sAddedOne: Column = f.transform(sArrCol, x => x + 1)
// sAddedOne: Column = transform(arr, lambdafunction((x_0 + 1), x_0))
val sAddedAll: Column = f.aggregate(sArrCol, f.lit(0), (x, y) => x + y)
// sAddedAll: Column = aggregate(arr, 0, lambdafunction((x_1 + y_2), x_1, y_2), lambdafunction(x_3, x_3))

dfArrays.select(sAddedAll as "complexTransformation").show
// +---------------------+
// |complexTransformation|
// +---------------------+
// |                    6|
// +---------------------+
//
```
This complex transformation has to ve developed in steps, and usually tested step by step. Also, the one line version is not easy to read
```scala
val complexS = f.aggregate(f.transform(f.col("arr"), x => x + 1), f.lit(0), (x, y) => x + y)
// complexS: Column = aggregate(transform(arr, lambdafunction((x_4 + 1), x_4)), 0, lambdafunction((x_5 + y_6), x_5, y_6), lambdafunction(x_7, x_7))

dfArrays.select(complexS as "complexTransformation").show
// +---------------------+
// |complexTransformation|
// +---------------------+
// |                    9|
// +---------------------+
//
```

Doric's way
```scala
val dArrCol: DoricColumn[Array[Int]] = col[Array[Int]]("arr")
// dArrCol: DoricColumn[Array[Int]] = NamedDoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$1471/1605170188@9e03c6e),
//   "arr"
// )
val dAddedOne: DoricColumn[Array[Int]] = dArrCol.transform(x => x + 1.lit)
// dAddedOne: DoricColumn[Array[Int]] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$1475/2107284848@378ffded)
// )
val dAddedAll: DoricColumn[Int] = dAddedOne.aggregate[Int](0.lit)((x, y) => x + y)
// dAddedAll: DoricColumn[Int] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$1475/2107284848@51787bc1)
// )

dfArrays.select(dAddedOne as "complexTransformation").show
// +---------------------+
// |complexTransformation|
// +---------------------+
// |            [2, 3, 4]|
// +---------------------+
//
```
We know all the time what type of data we will have, so is much easier to keep track of what we can do, and simplify the line o a single:
```scala
val complexCol: DoricColumn[Int] = col[Array[Int]]("arr")
  .transform(_ + 1.lit)
  .aggregate(0.lit)(_ + _)
// complexCol: DoricColumn[Int] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$1475/2107284848@734bb821)
// )
  
dfArrays.select(complexCol as "complexTransformation").show
// +---------------------+
// |complexTransformation|
// +---------------------+
// |                    9|
// +---------------------+
//
```

### Literal conversions
Sometimes spark allows adding direct literal values to simplify code
```scala
val intDF = List(1,2,3).toDF("int")
// intDF: org.apache.spark.sql.package.DataFrame = [int: int]
val colS = f.col("int") + 1
// colS: Column = (int + 1)

intDF.select(colS).show
// +---------+
// |(int + 1)|
// +---------+
// |        2|
// |        3|
// |        4|
// +---------+
//
```

Doric is a little stricter, forcing to transform this values to literal columns
```scala
val colD = colInt("int") + 1.lit
// colD: DoricColumn[Int] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$1475/2107284848@7bc0e96a)
// )

intDF.select(colD).show
// +---------+
// |(int + 1)|
// +---------+
// |        2|
// |        3|
// |        4|
// +---------+
//
```

This is de basic flavor to work with doric, but this obvious transformations can be simplified if we import an implicit conversion
```scala
import doric.implicitConversions.literalConversion
val colSugarD = colInt("int") + 1
// colSugarD: DoricColumn[Int] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$1475/2107284848@3368136a)
// )
val columConcatLiterals = concat("this", "is","doric") // concat expects DoricColumn[String] values, the conversion puts them as expected
// columConcatLiterals: StringColumn = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$1475/2107284848@21dcd0b5)
// ) // concat expects DoricColumn[String] values, the conversion puts them as expected

intDF.select(colSugarD, columConcatLiterals).show
// +---------+-----------------------+
// |(int + 1)|concat(this, is, doric)|
// +---------+-----------------------+
// |        2|            thisisdoric|
// |        3|            thisisdoric|
// |        4|            thisisdoric|
// +---------+-----------------------+
//
```

This conversion will transform any pure scala value, to its representation in a doric column, only if the type is valid
```scala
colInt("int") + 1f //an integer with a float value cant be directly added in doric
// error: type mismatch;
//  found   : Float(1.0)
//  required: doric.DoricColumn[Int]
// colInt("int") + 1f //an integer with a float value cant be directly added in doric
//                 ^^
```
```scala
concat("hi", 5) // expects only strings and a integer is found
// error: type mismatch;
//  found   : Int(5)
//  required: doric.StringColumn
//     (which expands to)  doric.DoricColumn[String]
// concat("hi", 5) // expects only strings and a integer is found
//              ^
```
