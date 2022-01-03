---
title: Doric Documentation
permalink: docs/
---

# Introduction to Doric
## First steps with Doric

Doric is very easy to work with, follow the [installation guide]({{ site.baseurl }}{% link docs/installation.md %})
first.

Then just import doric to your code

```scala mdoc:invisible

val spark = doric.DocInit.getSpark
```

```scala mdoc
import doric._
```

If you have basic knowledge of spark you have to change the way to reference to columns, first, introducing a concept similar to the `$` interpolator of spark.
```scala mdoc
val untypedCol: CName = c"my_column"
```
This is an element that represent the name of a column. The reason is to differentiate any element of type string that can be a literal
to a real column name. But in contrast to spark, this is not a valid column reference in doric, because in doric we need also the expected type of the column.
This can be done with the method `col`, indicating the type, of with less code with the apply method of the CName

```scala mdoc
// col method
val stringCol: DoricColumn[String] = col[String](c"str")
// apply method for CName
val stringColApply: DoricColumn[String] = c"str".apply[String]
// implicit conversion for CName
val stringColImpl: DoricColumn[String] = c"str"
```

And use this references as normal spark columns with the provided `select` and `withColumn` methods.

```scala mdoc

import spark.implicits._

val df = List("hi", "welcome", "to", "doric").toDF("str")

df.select(stringCol, stringColApply).show()
```

Doric adds an extra layer of knowledge to your column assigning a type, that is checked against the spark datatype in the
dataframe. As in the case of a dataframe that we ask the wrong column name, doric will also detect that the type is not
the expected.

```scala mdoc:crash
val wrongName = col[String](c"string")
df.select(wrongName)
```

```scala mdoc:crash
val wrongType = col[Int](c"str")
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
val stringPlusInt = col[Int](c"int") + col[String](c"str")
```
 This way we won't have any kind of unexpected behaviour in our process.
 
## Mix doric and spark
Doric only adds method to your everyday Spark Dataframe, you can mix spark selects and doric.

```scala mdoc
import org.apache.spark.sql.{functions => f}
df
  .select(f.concat(f.col("str"), f.lit("!!!")) as "newCol") //pure spark
  .select(concat(lit("???"), colString(c"newCol")) as c"finalCol") //pure and sweet doric
  .show()
```

Also, if you don't want to use doric to transform a column, you can transform a pure spark column into doric, and be sure that the type of the transformation is ok.
```scala mdoc
df.select(f.col("str").asDoric[String]).show()
```

But we recommend to use always the columns selectors from doric to prevent errors that doric can detect in compile time
```scala mdoc:crash
val sparkToDoricColumn = (f.col("str") + f.lit(true)).asDoric[String]
df.select(sparkToDoricColumn).show
```

In spark the sum of a string with a boolean will throw an error in runtime. In doric this code won't be able to compile.
```scala mdoc:fail
col[String](c"str") + true.lit
```

## Sweet doric syntax sugar
### Column selector alias
We know that doric can be seen as an extra boilerplate to get the columns, that's why we provide some extra methods to acquire the columns.
```scala mdoc
colString(c"str") // similar to col[String]("str")
colInt(c"int") // similar to col[Int]("int")
colArray[Int](c"int") // similar to col[Array[Int]]("int")
```
### Readable syntax
Doric tries to be less SQL verbose, and adopt a more object-oriented API, allowing the developer to view with the dot notation of scala the methods that can be used.
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
val dArrCol: DoricColumn[Array[Int]] = col[Array[Int]](c"arr")
val dAddedOne: DoricColumn[Array[Int]] = dArrCol.transform(x => x + 1.lit)
val dAddedAll: DoricColumn[Int] = dAddedOne.aggregate[Int](0.lit)((x, y) => x + y)

dfArrays.select(dAddedOne as c"complexTransformation").show
```
We know all the time what type of data we will have, so is much easier to keep track of what we can do, and simplify the line o a single:
```scala mdoc
val complexCol: DoricColumn[Int] = col[Array[Int]](c"arr")
  .transform(_ + 1.lit)
  .aggregate(0.lit)(_ + _)
  
dfArrays.select(complexCol as c"complexTransformation").show
```

### Literal conversions
Sometimes spark allows adding direct literal values to simplify code
```scala mdoc

val intDF = List(1,2,3).toDF("int")
val colS = f.col("int") + 1

intDF.select(colS).show
```

Doric is a little stricter, forcing to transform this values to literal columns
```scala mdoc
val colD = colInt(c"int") + 1.lit

intDF.select(colD).show
```

This is de basic flavor to work with doric, but this obvious transformations can be simplified if we import an implicit conversion
```scala mdoc
import doric.implicitConversions.literalConversion
val colSugarD = colInt(c"int") + 1
val columConcatLiterals = concat("this", "is","doric") // concat expects DoricColumn[String] values, the conversion puts them as expected

intDF.select(colSugarD, columConcatLiterals).show
```

This conversion will transform any pure scala value, to its representation in a doric column, only if the type is valid
```scala mdoc:fail
colInt("int") + 1f //an integer with a float value cant be directly added in doric
```
```scala mdoc:fail
concat("hi", 5) // expects only strings and a integer is found
```
