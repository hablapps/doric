---
title: Doric Documentation
permalink: docs/
---

# Introduction to doric

Doric is very easy to work with, follow the [installation guide]({{ site.baseurl }}{% link docs/installation.md %})
first.

Then just import doric to your code

```scala mdoc:invisible
import org.apache.spark.sql.{SparkSession, DataFrame}

val spark = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .appName("spark session")
      .getOrCreate()
      
```

```scala mdoc
import doric._
```

If you have basic knowlege of spark you have to change the way to reference to columns, only adding the expected type of
the column.

```scala mdoc
val stringCol = col[String]("str")
```

And use this references as normal spark columns with the provided `select` and `withColumn` methods.

```scala mdoc

import spark.implicits._

val df = List("hi", "welcome", "to", "doric").toDF("str")

df.select(stringCol).show()
```

Doric adds a extra layer of knowledge to your column assigning a type, that is checked against the spark datatype in the
dataframe. As in the case of a dataframe that we ask the wrong column name, doric will also detect that the type is not
the expected.

```scala mdoc:crash
val wrongName = col[String]("string")
df.select(wrongName)
```

```scala mdoc:crash
val wrongType = col[Int]("string")
df.select(wrongType)
```
This type of errors are obtained in runtime, but now that we know the exact type of the column,
we can operate according to the type.

```scala mdoc
val concatCol = concat(stringCol, stringCol)
df.select(concatCol).show()
```

And won't allow to do any operation that is not logic in compile time.
```scala mdoc:fail
val stringPlusInt = col[Int]("int") + col[String]("str")
```
 This way we won't have any kind of unexpected behaviour in our process.
 
## Doric doesn't force you to use it all the time
Doric only adds method to your everyday Spark Dataframe, you can mix spark selects and doric.

```scala mdoc
import org.apache.spark.sql.{functions => f}
df
  .select(f.concat(f.col("str"), f.lit("!!!")) as "newCol") //pure spark
  .select(concat(lit("???"), colString("newCol")) as "finalCol") //pure and sweet doric
  .show()
```

Also, if you don't want to use doric to transform a column, you can transform a pure spark column into doric, and be sure that the type of the transformation is ok.
```scala mdoc
df.select(f.col("str").asDoric[String]).show()
```

But we recomend to use allways the columns selectors from doric to prevent errors that doric can detect in compile time
```scala mdoc:crash
val sparkToDoricColumn = (f.col("str") + f.lit(true)).asDoric[String]
df.select(sparkToDoricColumn).show
```

In spark the sum of a string with a boolean will throw an error in runtime. In doric this code won't be able to compile.
```scala mdoc:fail
col[String]("str") + true.lit
```

## Sweet doric syntax sugar
### Column selector alias
We know that doric can be seen as an extra boilerplate to get the columns, that's why we provide some extra methods to aquire the columns.
```scala mdoc
colString("str") // similar to col[String]("str")
colInt("int") // similar to col[Int]("int")
colArray[Int]("int") // similar to col[Array[Int]]("int")
```

Doric tries to be less SQL verbose, and adopt a more object oriented API, allowing the developer to view with the dot notation of scala the methods that can be used.
```scala mdoc
val dfArrays = List(("string", Array(1,2,3))).toDF("str", "arr")
```
Spark SQL method-argument way to develop
```scala mdoc
import org.apache.spark.sql.Column

val sArrCol: Column = f.col("arr")
val sAddedOne: Column = f.transform(sArrCol, x => x + 1)
val sAddedAll: Column = f.aggregate(sArrCol, f.lit(0), (x, y) => x + y)

dfArrays.select(sAddedAll as "complexTransformation").show
```
This complex transformation has to ve developed in steps, and usually tested step by step. Also, the one line version is not easy to read
```scala mdoc
val complexS = f.aggregate(f.transform(f.col("arr"), x => x + 1), f.lit(0), (x, y) => x + y)

dfArrays.select(complexS as "complexTransformation").show
```

Doric's way
```scala mdoc
val dArrCol: DoricColumn[Array[Int]] = col[Array[Int]]("arr")
val dAddedOne: DoricColumn[Array[Int]] = dArrCol.transform(x => x + 1.lit)
val dAddedAll: DoricColumn[Int] = dAddedOne.aggregate[Int](0.lit)((x, y) => x + y)

dfArrays.select(dAddedOne as "complexTransformation").show
```
We know all the time what type of data we will have, so is much easier to keep track of what we can do, and simplify the line o a single:
```scala mdoc
val complexCol: DoricColumn[Int] = col[Array[Int]]("arr")
  .transform(_ + 1.lit)
  .aggregate(0.lit)(_ + _)
  
dfArrays.select(complexCol as "complexTransformation").show
```

### Literal conversions
Some times spark allows to add direct literal values to simplify code
```scala mdoc

val intDF = List(1,2,3).toDF("int")
val colS = f.col("int") + 1

intDF.select(colS).show
```

Doric is a little more strict, forcing to transform this values to literal columns
```scala mdoc
val colD = colInt("int") + 1.lit

intDF.select(colD).show
```

This is de basic flavor to work with doric, but this obvious transformations can be suggarice if we import an implicit conversion
```scala mdoc
import doric.implicitConversions.literalConversion
val colSugarD = colInt("int") + 1
val columConcatLiterals = concat("this", "is","doric") // concat expects DoricColumn[String] values, the conversion puts them as expected

intDF.select(colSugarD, columConcatLiterals).show
```

This conversion will transform any pure scala value, to it's representation in a doric column, only if the type is valid
```scala mdoc:fail
colInt("int") + 1f //an integer with a float value cant be directly added in doric
```
```scala mdoc:fail
concat("hi", 5) // expects only strings and a integer is found
```
