---
title: Advanced functions'
permalink: docs/advanced
---
```scala mdoc:invisible
import org.apache.spark.sql.{DataFrame}

val spark = doric.DocInit.getSpark
      
import spark.implicits._
```
# Advanced functions
Doric is a higher order API, and the doric column, a combination of the functional programming and spark, allows us to develop much richer operations.

## When builder
In difference to the spark function `when`, doric provides a builder for this operator that will ensure the type of the condition result.

```scala mdoc
import doric._
import doric.implicitConversions.literalConversion

val ex1: DoricColumn[String] =
  when[String] //declare the when with the expected return type
    .caseW(col[Int]("int_col") > 3, "big") // conditions always are boolean, and the return type is checked
    .otherwise("small") // 
  
val ex2: DoricColumn[String] =
  when[String] //declare the when with the expected return type
    .caseW(col[Int]("int_col") > 5, "big") // conditions always are boolean, and the return type is checked
    .caseW(col[Int]("int_col") > 0, "small")
    .otherwiseNull // know that are cases that it can return null values
  
List(0, 1, 2, 3, 4, 5).toDF("int_col")
  .withNamedColumns(
    ex1.as("ex1"),
    ex2.as("ex2")
  ).show()
```

## Column Type Matcher
The introduction of type checking in doric makes the API more robust. If there is a moment that the developer doesn't know for sure the expected type, it can use the type matcher to make more readable the logic. The Type matcher operator method allows to condition the logic to apply to a column depending on the type that it is.
```scala mdoc
val matchEx1: DoricColumn[Int] = matchToType[Int]("my_column") // we only know the name that must exist in the dataframe
  .caseType[Int](identity) // if in case that it's already a Int column, keep it as it is
  .caseType[String](_.unsafeCast) // if it's a String type try to make a unsafe cast
  .caseType[Array[Int]](_.getIndex(0) + 10) // a complex transformation in case it's an array of integers
  .inOtherCaseError // in any other case, it will produce an error that will be displayed as any other [doric errors](/docs/errors/)
```
Then you can use like the normal doric column it is

```scala mdoc
List(1,2,3).toDF("my_column").select(matchEx1.as("isInt")).show()
List("1", "2", "c").toDF("my_column").select(matchEx1.as("isStr")).show()
List(List(1, 2), List(3), List()).toDF("my_column").select(matchEx1.as("isArrInt")).show()
```
And inf you have an error with your data, it will point to the `inOtherCaseError` that is responsible.
```scala mdoc:crash
List(1L,2L,3L).toDF("my_column").select(matchEx1.as("isErrorLong"))
```
If you always want to return a value, you can end the matchToType with a default value

```scala mdoc
val matchEx2 = matchToType[Int]("my_column")
  .caseType[Int](identity)
  .inOtherCase(-1)
```

```scala mdoc
List(1,2,3).toDF("my_column").select(matchEx2.as("isInt")).show()
List("1", "2", "c").toDF("my_column").select(matchEx2.as("isDef")).show()
```
