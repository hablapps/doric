---
title: Doric Documentation
permalink: docs/syntax/
---


## Sweet doric syntactic sugar

Before delving into the specific topics of [validations](validations.md) 
and [modularity](modularity.md), let's discuss some general considerations around syntax. 

### Dot syntax

Doric embraces the _dot notation_ of common idiomatic Scala code wherever possible, instead of the functional style of Spark SQL. For instance, given the following DataFrame:
```scala
val dfArrays = List(("string", Array(1,2,3))).toDF("str", "arr")
// dfArrays: org.apache.spark.sql.package.DataFrame = [str: string, arr: array<int>]
```

a common transformation in the SQL style would go as follows:

```scala
val complexS: Column = 
    f.aggregate(
        f.transform(f.col("arr"), x => x + 1), 
        f.lit(0), 
        (x, y) => x + y)
// complexS: Column = aggregate(transform(arr, lambdafunction((x_0 + 1), x_0)), 0, lambdafunction((x_1 + y_2), x_1, y_2), lambdafunction(x_3, x_3))

dfArrays.select(complexS as "complexTransformation").show
// +---------------------+
// |complexTransformation|
// +---------------------+
// |                    9|
// +---------------------+
//
```

Using doric, we would write it like this:
```scala
val complexCol: DoricColumn[Int] = 
    col[Array[Int]]("arr")
      .transform(_ + 1.lit)
      .aggregate(0.lit)(_ + _)
// complexCol: DoricColumn[Int] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$2837/0x000000010121b840@78c34153)
// )
  
dfArrays.select(complexCol as "complexTransformation").show
// +---------------------+
// |complexTransformation|
// +---------------------+
// |                    9|
// +---------------------+
//
```

### Implicit castings

Implicit type conversions in Spark are pervasive. For instance, the following code won't cause Spark to complain at all:

```scala
val df0 = spark.range(1,10).withColumn("x", f.concat(f.col("id"), f.lit("jander")))
// df0: org.apache.spark.sql.package.DataFrame = [id: bigint, x: string]
```

which means that an implicit conversion from bigint to string will be in effect when we run our DataFrame:

```scala
df0.select(f.col("x")).show
// +-------+
// |      x|
// +-------+
// |1jander|
// |2jander|
// |3jander|
// |4jander|
// |5jander|
// |6jander|
// |7jander|
// |8jander|
// |9jander|
// +-------+
//
```

Assuming that you are certain that your column holds vales of type bigint, the same code in doric won't compile
(note that the Spark type `Bigint` corresponds to the Scala type `Long`):

```scala
val df1 = spark.range(1,10).toDF.withColumn("x", concat(colLong("id"), "jander".lit))
// error: type mismatch;
//  found   : doric.NamedDoricColumn[Long]
//  required: doric.StringColumn
//     (which expands to)  doric.DoricColumn[String]
// val df1 = spark.range(1,10).toDF.withColumn("x", concat(colLong("id"), "jander".lit))
//                                                                ^
```

Still, doric will allow you to perform that operation provided that you explicitly enact the conversion:

```scala
val df1 = spark.range(1,10).toDF.withColumn("x", concat(colLong("id").cast[String], "jander".lit))
// df1: org.apache.spark.sql.package.DataFrame = [id: bigint, x: string]
df1.show
// +---+-------+
// | id|      x|
// +---+-------+
// |  1|1jander|
// |  2|2jander|
// |  3|3jander|
// |  4|4jander|
// |  5|5jander|
// |  6|6jander|
// |  7|7jander|
// |  8|8jander|
// |  9|9jander|
// +---+-------+
//
```

Let's also consider the following example:

```scala
val dfEq = List((1, "1"), (1, " 1"), (1, " 1 ")).toDF("int", "str")
// dfEq: org.apache.spark.sql.package.DataFrame = [int: int, str: string]
dfEq.withColumn("eq", f.col("int") === f.col("str"))
// res5: org.apache.spark.sql.package.DataFrame = [int: int, str: string ... 1 more field]
```

What would you expect to be the result? Well, it all depends on the implicit conversion that Spark chooses to apply, 
if at all: 
1. It may return false for the new column, given that the types of both input columns differ, 
thus choosing to apply no conversion
2. It may convert the integer column into a string column
3. It may convert strings to integers. 

Let's see what happens:

```scala
dfEq.withColumn("eq", f.col("int") === f.col("str")).show
// +---+---+----+
// |int|str|  eq|
// +---+---+----+
// |  1|  1|true|
// |  1|  1|true|
// |  1| 1 |true|
// +---+---+----+
//
```

Option 3 wins, but you can only learn this by trial and error. With doric, you can depart from all this magic and 
explicitly cast types, if you desired so:

```scala
// Option 1, no castings: compile error
dfEq.withColumn("eq", colInt("int") === colString("str")).show
// error: type mismatch;
//  found   : doric.NamedDoricColumn[String]
//  required: doric.DoricColumn[Int]
// dfEq.withColumn("eq", colInt("int") === colString("str")).show
//                                                  ^
```

```scala
// Option 2, casting from int to string
dfEq.withColumn("eq", colInt("int").cast[String] === colString("str")).show
// +---+---+-----+
// |int|str|   eq|
// +---+---+-----+
// |  1|  1| true|
// |  1|  1|false|
// |  1| 1 |false|
// +---+---+-----+
//
```

```scala
// Option 3, casting from string to int, not safe!
dfEq.withColumn("eq", colInt("int") === colString("str").unsafeCast[Int]).show
// +---+---+----+
// |int|str|  eq|
// +---+---+----+
// |  1|  1|true|
// |  1|  1|true|
// |  1| 1 |true|
// +---+---+----+
//
```

Note that we can't simply cast a string to an integer, since this conversion is partial. If the programmer insists 
in doing this unsafe casting, doric will force her to explicitly acknowledge this fact using the conversion function 
`unsafeCast`.

Last, note that we can also emulate the default Spark behaviour, enabling implicit conversions for safe castings, 
with an explicit import statement:

```scala
import doric.implicitConversions.implicitSafeCast

dfEq.withColumn("eq", colString("str") === colInt("int") ).show
// +---+---+-----+
// |int|str|   eq|
// +---+---+-----+
// |  1|  1| true|
// |  1|  1|false|
// |  1| 1 |false|
// +---+---+-----+
//
```

### Literal conversions

Sometimes, Spark allows us to insert literal values to simplify our code:

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

The default doric syntax is a little stricter and forces us to transform these values to literal columns:

```scala
val colD = colInt("int") + 1.lit
// colD: DoricColumn[Int] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$2837/0x000000010121b840@70c6860)
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

However, we can also profit from the same literal syntax with the help of implicits. To enable this behaviour,
we have to _explicitly_ add the following import statement:

```scala
import doric.implicitConversions.literalConversion
val colSugarD = colInt("int") + 1
// colSugarD: DoricColumn[Int] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$2837/0x000000010121b840@7ef6a063)
// )
val columConcatLiterals = concat("this", "is","doric") // concat expects DoricColumn[String] values, the conversion puts them as expected
// columConcatLiterals: StringColumn = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$2837/0x000000010121b840@1cd7bc66)
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

Of course, implicit conversions are only in effect if the type of the literal value is valid:
```scala
colInt("int") + 1f //an integer with a float value can't be directly added in doric
// error: type mismatch;
//  found   : Float(1.0)
//  required: doric.DoricColumn[Int]
// colInt("int") + 1f //an integer with a float value can't be directly added in doric
//                 ^^
```
```scala
concat("hi", 5) // expects only strings and an integer is found
// error: type mismatch;
//  found   : Int(5)
//  required: doric.StringColumn
//     (which expands to)  doric.DoricColumn[String]
// concat("hi", 5) // expects only strings and an integer is found
//              ^
```

