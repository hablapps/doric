---
title: Pinpointing the error place
permalink: docs/errors/
---

# Errors in doric
## Error aggregation
Doric is a typesafe API, this means that the compiler will prevent to do transformations that will throw exceptions in runtime.
The only possible source of errors is in the selection of columns, if the column doesn't exist or if the column contains an unexpected type.

Let's see an example of an error
```scala
import doric._

val df = List(("hi", 31)).toDF("str", "int")
// df: DataFrame = [str: string, int: int]
val col1 = colInt("str")
// col1: DoricColumn[Int] = DoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$2701/910656502@40013051)
// )
val col2 = colString("int")
// col2: DoricColumn[String] = DoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$2701/910656502@45849604)
// )
val col3 = colInt("unknown")
// col3: DoricColumn[Int] = DoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$2701/910656502@6cbbfe29)
// )
```
```scala
df.select(col1, col2, col3)
// doric.sem.DoricMultiError: Found 3 errors in select
//   The column with name 'str' is of type StringType and it was expected to be IntegerType
//   	located at . (error-location.md:31)
//   The column with name 'int' is of type IntegerType and it was expected to be StringType
//   	located at . (error-location.md:34)
//   Cannot resolve column name "unknown" among (str, int)
//   	located at . (error-location.md:37)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:120)
// 	at repl.MdocSession$App$$anonfun$6.apply(error-location.md:44)
// 	at repl.MdocSession$App$$anonfun$6.apply(error-location.md:44)
```

The select statement throws a single exception, and it contains 3 different errors.
Each error points to the place were the error is declared, the column selector line.
This is the way that doric makes faster and easier to solve common development problems.

## How to get the most of Doric error location
Doric main focus is to give the possibility of modularize the logic for the etl, and the idea to find fast the location of an error is essential.
Let's make an example, imagine that we have some columns have a suffix in the name telling us is information of a user.


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

Us as developers want to abstract from this suffix and focus only in the unique part of the name:
```scala
colString("name_user")
// res2: DoricColumn[String] = DoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$2701/910656502@2f61de04)
// )
colInt("age_user")
// res3: DoricColumn[Int] = DoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$2701/910656502@5f5a33ed)
// )
colString("city_user")
// res4: DoricColumn[String] = DoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$2701/910656502@5b59c3d)
// )
```
So we can make a function to simplify it:
```scala
import doric.types.SparkType
def user[T: SparkType](colName: String): DoricColumn[T] = {
  col[T](colName + "_user")
}
```
In valid cases it works ok, bug when an error is produce because one of these references, it will point to the line `col[T](colName + "_user")` that is not the real problem.

```scala
val userc = user[Int]("name") //wrong type :S
userDF.select(userc)
// doric.sem.DoricMultiError: Found 1 error in select
//   The column with name 'name_user' is of type StringType and it was expected to be IntegerType
//   	located at . (error-location.md:88)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:120)
// 	at repl.MdocSession$App$$anonfun$14.apply(error-location.md:97)
// 	at repl.MdocSession$App$$anonfun$14.apply(error-location.md:95)
```

What we really want is to mark as the source the place we are using our `user` method. We can achieve this by adding only an implicit value to the definition:

```scala
import doric._
import doric.sem.Location
import doric.types.SparkType

def user[T: SparkType](colName: String)(implicit location: Location): DoricColumn[T] = {
  col[T](colName + "_user")
}
```
Now if we repeat the same error we will be pointed to the real problem
```scala
val age = user[Int]("name")
val team = user[String]("team")
userDF.select(age, team)
// doric.sem.DoricMultiError: Found 2 errors in select
//   The column with name 'name_user' is of type StringType and it was expected to be IntegerType
//   	located at . (error-location.md:151)
//   Cannot resolve column name "team_user" among (name_user, city_user, age_user)
//   	located at . (error-location.md:152)
// 
// 	at doric.sem.package$ErrorThrower.$anonfun$returnOrThrow$1(package.scala:9)
// 	at cats.data.Validated.fold(Validated.scala:29)
// 	at doric.sem.package$ErrorThrower.returnOrThrow(package.scala:9)
// 	at doric.sem.TransformOps$DataframeTransformationSyntax.select(TransformOps.scala:120)
// 	at repl.MdocSession$App5$$anonfun$18.apply(error-location.md:153)
// 	at repl.MdocSession$App5$$anonfun$18.apply(error-location.md:150)
```
