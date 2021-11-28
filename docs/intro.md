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

If you have basic knowledge of spark you have to change the way to reference to columns, first, introducing a concept similar to the `$` interpolator of spark.
```scala
val untypedCol: CName = c"my_column"
// untypedCol: CName = "my_column"
```
This is an element that represent the name of a column. The reason is to differentiate any element of type string that can be a literal
to a real column name. But in contrast to spark, this is not a valid column reference in doric, because in doric we need also the expected type of the column.
This can be done with the method `col`, indicating the type, of with less code with the apply method of the CName

```scala
// col method
val stringCol: DoricColumn[String] = col[String](c"str")
// stringCol: DoricColumn[String] = NamedDoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$2284/921284068@392da875),
//   "str"
// )
// apply method for CName
val stringColApply: DoricColumn[String] = c"str".apply[String]
// stringColApply: DoricColumn[String] = NamedDoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$2284/921284068@68603829),
//   "str"
// )
// implicit conversion for CName
val stringColImpl: DoricColumn[String] = c"str"
// stringColImpl: DoricColumn[String] = NamedDoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$2284/921284068@6e2ab1f4),
//   "str"
// )
```

And use this references as normal spark columns with the provided `select` and `withColumn` methods.

```scala
import spark.implicits._

val df = List("hi", "welcome", "to", "doric").toDF("str")
// df: DataFrame = [str: string]

df.select(stringCol, stringColApply).show()
// +-------+-------+
// |    str|    str|
// +-------+-------+
// |     hi|     hi|
// |welcome|welcome|
// |     to|     to|
// |  doric|  doric|
// +-------+-------+
//
```

Doric adds an extra layer of knowledge to your column assigning a type, that is checked against the spark datatype in the
dataframe. As in the case of a dataframe that we ask the wrong column name, doric will also detect that the type is not
the expected.

```scala
val wrongName = col[String](c"string")
df.select(wrongName)
// doric.sem.DoricMultiError: Found 1 error in select
//   Cannot resolve column name "string" among (str)
//   	located at . (intro.md:59)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:137)
// 	at repl.MdocSession$App$$anonfun$8.apply(intro.md:60)
// 	at repl.MdocSession$App$$anonfun$8.apply(intro.md:58)
// Caused by: org.apache.spark.sql.AnalysisException: Cannot resolve column name "string" among (str)
// 	at org.apache.spark.sql.Dataset.org$apache$spark$sql$Dataset$$resolveException(Dataset.scala:272)
// 	at org.apache.spark.sql.Dataset.$anonfun$resolve$1(Dataset.scala:263)
// 	at scala.Option.getOrElse(Option.scala:189)
// 	at org.apache.spark.sql.Dataset.resolve(Dataset.scala:263)
// 	at org.apache.spark.sql.Dataset.col(Dataset.scala:1359)
// 	at org.apache.spark.sql.Dataset.apply(Dataset.scala:1326)
// 	at doric.types.SparkType.$anonfun$validate$1(SparkType.scala:45)
// 	at cats.data.Kleisli.$anonfun$map$1(Kleisli.scala:40)
// 	at cats.data.Kleisli.$anonfun$map$1(Kleisli.scala:40)
// 	at cats.data.Kleisli.$anonfun$map$1(Kleisli.scala:40)
```

```scala
val wrongType = col[Int](c"str")
df.select(wrongType)
// doric.sem.DoricMultiError: Found 1 error in select
//   The column with name 'str' is of type StringType and it was expected to be IntegerType
//   	located at . (intro.md:70)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:137)
// 	at repl.MdocSession$App$$anonfun$10.apply(intro.md:71)
// 	at repl.MdocSession$App$$anonfun$10.apply(intro.md:69)
```
This type of errors are obtained in runtime, but now that we know the exact type of the column,
we can operate according to the type.

```scala
val concatCol = concat(stringCol, stringCol)
// concatCol: StringColumn = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$2366/1438662545@5ca4763f)
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
val stringPlusInt = col[Int](c"int") + col[String](c"str")
// error: type mismatch;
//  found   : doric.NamedDoricColumn[String]
//  required: doric.DoricColumn[Int]
// val stringPlusInt = col[Int](c"int") + col[String](c"str")
//                                                   ^
```
 This way we won't have any kind of unexpected behaviour in our process.
 
## Mix doric and spark
Doric only adds method to your everyday Spark Dataframe, you can mix spark selects and doric.

```scala
import org.apache.spark.sql.{functions => f}
df
  .select(f.concat(f.col("str"), f.lit("!!!")) as "newCol") //pure spark
  .select(concat(lit("???"), colString(c"newCol")) as c"finalCol") //pure and sweet doric
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
//   'Project [(cast(str#171 as double) + true) AS (str + true)#214]
//   +- Project [value#168 AS str#171]
//      +- LocalRelation [value#168]
//   
//   	located at . (intro.md:114)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:137)
// 	at repl.MdocSession$App$$anonfun$17.apply$mcV$sp(intro.md:115)
// 	at repl.MdocSession$App$$anonfun$17.apply(intro.md:113)
// 	at repl.MdocSession$App$$anonfun$17.apply(intro.md:113)
// Caused by: org.apache.spark.sql.AnalysisException: cannot resolve '(CAST(`str` AS DOUBLE) + true)' due to data type mismatch: differing types in '(CAST(`str` AS DOUBLE) + true)' (double and boolean).;
// 'Project [(cast(str#171 as double) + true) AS (str + true)#214]
// +- Project [value#168 AS str#171]
//    +- LocalRelation [value#168]
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
col[String](c"str") + true.lit
// error: type mismatch;
//  found   : doric.DoricColumn[Boolean]
//  required: doric.StringColumn
//     (which expands to)  doric.DoricColumn[String]
// col[String](c"str") + true.lit
//                       ^^^^^^^^
```

## Sweet doric syntax sugar
### Column selector alias
We know that doric can be seen as an extra boilerplate to get the columns, that's why we provide some extra methods to acquire the columns.
```scala
colString(c"str") // similar to col[String]("str")
// res6: NamedDoricColumn[String] = NamedDoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$2284/921284068@33dcbbfa),
//   "str"
// ) // similar to col[String]("str")
colInt(c"int") // similar to col[Int]("int")
// res7: NamedDoricColumn[Int] = NamedDoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$2284/921284068@57d3c5cd),
//   "int"
// ) // similar to col[Int]("int")
colArray[Int](c"int") // similar to col[Array[Int]]("int")
// res8: NamedDoricColumn[Array[Int]] = NamedDoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$2284/921284068@5418225f),
//   "int"
// )
```
### Readable syntax
Doric tries to be less SQL verbose, and adopt a more object-oriented API, allowing the developer to view with the dot notation of scala the methods that can be used.
```scala
val dfArrays = List(("string", Array(1,2,3))).toDF("str", "arr")
// dfArrays: DataFrame = [str: string, arr: array<int>]
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
val dArrCol: DoricColumn[Array[Int]] = col[Array[Int]](c"arr")
// dArrCol: DoricColumn[Array[Int]] = NamedDoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$2284/921284068@3369a71f),
//   "arr"
// )
val dAddedOne: DoricColumn[Array[Int]] = dArrCol.transform(x => x + 1.lit)
// dAddedOne: DoricColumn[Array[Int]] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$2366/1438662545@7adde112)
// )
val dAddedAll: DoricColumn[Int] = dAddedOne.aggregate[Int](0.lit)((x, y) => x + y)
// dAddedAll: DoricColumn[Int] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$2366/1438662545@6f422ecc)
// )

dfArrays.select(dAddedOne as c"complexTransformation").show
// +---------------------+
// |complexTransformation|
// +---------------------+
// |            [2, 3, 4]|
// +---------------------+
//
```
We know all the time what type of data we will have, so is much easier to keep track of what we can do, and simplify the line o a single:
```scala
val complexCol: DoricColumn[Int] = col[Array[Int]](c"arr")
  .transform(_ + 1.lit)
  .aggregate(0.lit)(_ + _)
// complexCol: DoricColumn[Int] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$2366/1438662545@6c648d16)
// )
  
dfArrays.select(complexCol as c"complexTransformation").show
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
// intDF: DataFrame = [int: int]
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
val colD = colInt(c"int") + 1.lit
// colD: DoricColumn[Int] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$2366/1438662545@12cb9eda)
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
val colSugarD = colInt(c"int") + 1
// colSugarD: DoricColumn[Int] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$2366/1438662545@70b38e0)
// )
val columConcatLiterals = concat("this", "is","doric") // concat expects DoricColumn[String] values, the conversion puts them as expected
// columConcatLiterals: StringColumn = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$2366/1438662545@79414283)
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
//  found   : String("int")
//  required: doric.CName
//     (which expands to)  doric.CName.Type
// colInt("int") + 1f //an integer with a float value cant be directly added in doric
//        ^^^^^
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
