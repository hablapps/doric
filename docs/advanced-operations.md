---
title: Advanced functions'
permalink: docs/advanced
---
# Advanced functions
Doric is a higher order API, and the doric column, a combination of the functional programming and spark, allows us to develop much richer operations.

## When builder
In difference to the spark function `when`, doric provides a builder for this operator that will ensure the type of the condition result.

```scala
import doric._
import doric.implicitConversions.literalConversion

val ex1: DoricColumn[String] =
  when[String] //declare the when with the expected return type
    .caseW(col[Int]("int_col") > 3, "big") // conditions always are boolean, and the return type is checked
    .otherwise("small") // 
// ex1: DoricColumn[String] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$1437/1333808934@25587290)
// ) // 
  
val ex2: DoricColumn[String] =
  when[String] //declare the when with the expected return type
    .caseW(col[Int]("int_col") > 5, "big") // conditions always are boolean, and the return type is checked
    .caseW(col[Int]("int_col") > 0, "small")
    .otherwiseNull // know that are cases that it can return null values
// ex2: DoricColumn[String] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$1437/1333808934@716eae1)
// ) // know that are cases that it can return null values
  
List(0, 1, 2, 3, 4, 5).toDF("int_col")
  .withNamedColumns(
    ex1.as("ex1"),
    ex2.as("ex2")
  ).show()
// +-------+-----+-----+
// |int_col|  ex1|  ex2|
// +-------+-----+-----+
// |      0|small| null|
// |      1|small|small|
// |      2|small|small|
// |      3|small|small|
// |      4|  big|small|
// |      5|  big|small|
// +-------+-----+-----+
//
```

## Column Type Matcher
The introduction of type checking in doric makes the API more robust. If there is a moment that the developer doesn't know for sure the expected type, it can use the type matcher to make more readable the logic. The Type matcher operator method allows to condition the logic to apply to a column depending on the type that it is.
```scala
val matchEx1: DoricColumn[Int] = matchToType[Int]("my_column") // we only know the name that must exist in the dataframe
  .caseType[Int](identity) // if in case that it's already a Int column, keep it as it is
  .caseType[String](_.unsafeCast) // if it's a String type try to make a unsafe cast
  .caseType[Array[Int]](_.getIndex(0) + 10) // a complex transformation in case it's an array of integers
  .inOtherCaseError // in any other case, it will produce an error that will be displayed as any other [doric errors](/docs/errors/)
// matchEx1: DoricColumn[Int] = TransformationDoricColumn(
//   Kleisli(doric.syntax.NonEmptyTypeMatcher$$Lambda$2952/2135864238@7ffe8a82)
// )
```
Then you can use like the normal doric column it is

```scala
List(1,2,3).toDF("my_column").select(matchEx1.as("isInt")).show()
// +-----+
// |isInt|
// +-----+
// |    1|
// |    2|
// |    3|
// +-----+
// 
List("1", "2", "c").toDF("my_column").select(matchEx1.as("isStr")).show()
// +-----+
// |isStr|
// +-----+
// |    1|
// |    2|
// | null|
// +-----+
// 
List(List(1, 2), List(3), List()).toDF("my_column").select(matchEx1.as("isArrInt")).show()
// +--------+
// |isArrInt|
// +--------+
// |      11|
// |      13|
// |    null|
// +--------+
//
```
And inf you have an error with your data, it will point to the `inOtherCaseError` that is responsible.
```scala
List(1L,2L,3L).toDF("my_column").select(matchEx1.as("isErrorLong"))
// doric.sem.DoricMultiError: Found 1 error in select
//   The matched column with name 'my_column' is of type IntegerType and it was expected to be one of [ArrayType(IntegerType,true), StringType, IntegerType]
//   	located at . (advanced-operations.md:53)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:139)
// 	at repl.MdocSession$App$$anonfun$4.apply(advanced-operations.md:72)
// 	at repl.MdocSession$App$$anonfun$4.apply(advanced-operations.md:72)
```
If you always want to return a value, you can end the matchToType with a default value

```scala
val matchEx2 = matchToType[Int]("my_column")
  .caseType[Int](identity)
  .inOtherCase(-1)
// matchEx2: DoricColumn[Int] = TransformationDoricColumn(
//   Kleisli(doric.syntax.NonEmptyTypeMatcher$$Lambda$2987/101378861@4a29a1e6)
// )
```

```scala
List(1,2,3).toDF("my_column").select(matchEx2.as("isInt")).show()
// +-----+
// |isInt|
// +-----+
// |    1|
// |    2|
// |    3|
// +-----+
// 
List("1", "2", "c").toDF("my_column").select(matchEx2.as("isDef")).show()
// +-----+
// |isDef|
// +-----+
// |   -1|
// |   -1|
// |   -1|
// +-----+
//
```
