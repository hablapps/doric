---
title: Doric Documentation
permalink: docs/syntax/
---

```scala mdoc:invisible
import org.apache.spark.sql.{Column, functions => f}

val spark = doric.DocInit.getSpark
import spark.implicits._

import doric._
```

## Sweet doric syntactic sugar

Before delving into the specific topics of [validations](validations.md) 
and [modularity](modularity.md), let's discuss some general considerations around syntax. 

### Dot syntax

Doric embraces the _dot notation_ of common idiomatic Scala code wherever possible, instead of the functional style of Spark SQL. For instance, given the following DataFrame:
```scala mdoc
val dfArrays = List(("string", Array(1,2,3))).toDF("str", "arr")
```

a common transformation in the SQL style would go as follows:

```scala mdoc
val complexS: Column = 
    f.aggregate(
        f.transform(f.col("arr"), x => x + 1), 
        f.lit(0), 
        (x, y) => x + y)

dfArrays.select(complexS as "complexTransformation").show
```

Using doric, we would write it like this:
```scala mdoc
val complexCol: DoricColumn[Int] = 
    col[Array[Int]]("arr")
      .transform(_ + 1.lit)
      .aggregate(0.lit)(_ + _)
  
dfArrays.select(complexCol as "complexTransformation").show
```

### Implicit castings

Implicit type conversions in Spark are pervasive. For instance, the following code won't cause Spark to complain at all:

```scala mdoc
val df0 = spark.range(1,10).withColumn("x", f.concat(f.col("id"), f.lit("jander")))
```

which means that an implicit conversion from bigint to string will be in effect when we run our DataFrame:

```scala mdoc
df0.select(f.col("x")).show
```

Assuming that you are certain that your column holds vales of type bigint, the same code in doric won't compile
(note that the Spark type `Bigint` corresponds to the Scala type `Long`):

```scala mdoc:fail
val df1 = spark.range(1,10).toDF.withColumn("x", concat(colLong("id"), "jander".lit))
```

Still, doric will allow you to perform that operation provided that you explicitly enact the conversion:

```scala mdoc
val df1 = spark.range(1,10).toDF.withColumn("x", concat(colLong("id").cast[String], "jander".lit))
df1.show
```

Let's also consider the following example:

```scala mdoc
val dfEq = List((1, "1"), (1, " 1"), (1, " 1 ")).toDF("int", "str")
dfEq.withColumn("eq", f.col("int") === f.col("str"))
```

What would you expect to be the result? Well, it all depends on the implicit conversion that Spark chooses to apply, 
if at all: 
1. It may return false for the new column, given that the types of both input columns differ, 
thus choosing to apply no conversion
2. It may convert the integer column into a string column
3. It may convert strings to integers. 

Let's see what happens:

```scala mdoc
dfEq.show
```

Option 3 wins, but you can only learn this by trial and error. With doric, you can depart from all this magic and 
explicitly cast types, if you desired so:

```scala mdoc:fail
// Option 1, no castings: compile error
dfEq.withColumn("eq", colInt("int") === colString("str")).show
```

```scala mdoc
// Option 2, casting from int to string
dfEq.withColumn("eq", colInt("int").cast[String] === colString("str")).show
```

```scala mdoc
// Option 3, casting from string to int, not safe!
dfEq.withColumn("eq", colInt("int") === colString("str").unsafeCast[Int]).show
```

Note that we can't simply cast a string to an integer, since this conversion is partial. If the programmer insists 
in doing this unsafe casting, doric will force her to explicitly acknowledge this fact using the conversion function 
`unsafeCast`.

Last, note that we can also emulate the default Spark behaviour, enabling implicit conversions for safe castings, 
with an explicit import statement:

```scala mdoc
import doric.implicitConversions.implicitSafeCast

dfEq.withColumn("eq", colString("str") === colInt("int") ).show
```

### Literal conversions

Sometimes, Spark allows us to insert literal values to simplify our code:

```scala mdoc

val intDF = List(1,2,3).toDF("int")
val colS = f.col("int") + 1

intDF.select(colS).show
```

The default doric syntax is a little stricter and forces us to transform these values to literal columns:

```scala mdoc
val colD = colInt("int") + 1.lit

intDF.select(colD).show
```

However, we can also profit from the same literal syntax with the help of implicits. To enable this behaviour,
we have to _explicitly_ add the following import statement:

```scala mdoc
import doric.implicitConversions.literalConversion
val colSugarD = colInt("int") + 1
val columConcatLiterals = concat("this", "is","doric") // concat expects DoricColumn[String] values, the conversion puts them as expected

intDF.select(colSugarD, columConcatLiterals).show
```

Of course, implicit conversions are only in effect if the type of the literal value is valid:
```scala mdoc:fail
colInt("int") + 1f //an integer with a float value can't be directly added in doric
```
```scala mdoc:fail
concat("hi", 5) // expects only strings and an integer is found
```

