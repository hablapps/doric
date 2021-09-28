---
title: Pinpointing the error place
permalink: docs/errors/
---

# Errors in doric
## Error aggregation
Doric is a typesafe API, this means that the compiler will prevent to do transformations that will throw exceptions in runtime.
The only possible source of errors is in the selection of columns, if the column doesn't exist or if the column contains an unexpected type.

```scala mdoc:invisible:reset

val spark = doric.DocInit.getSpark
      
import spark.implicits._
```
Let's see an example of an error
```scala mdoc
import doric._

val df = List(("hi", 31)).toDF("str", "int")
val col1 = colInt(c"str")
val col2 = colString(c"int")
val col3 = colInt(c"unknown")
```
```scala mdoc:crash
df.select(col1, col2, col3)
```

The select statement throws a single exception, and it contains 3 different errors.
Each error points to the place were the error is declared, the column selector line.
This is the way that doric makes faster and easier to solve common development problems.

## How to get the most of Doric error location
Doric main focus is to give the possibility of modularize the logic for the etl, and the idea to find fast the location of an error is essential.
Let's make an example, imagine that we have some columns have a suffix in the name telling us is information of a user.

```scala mdoc:invisible
val userDF = List(
("Foo", "Madrid", 35),
("Bar", "New York", 40),
("John", "Paris", 30)
).toDF("name_user", "city_user", "age_user")
```

```scala mdoc
userDF.show
userDF.printSchema
```

Us as developers want to abstract from this suffix and focus only in the unique part of the name:
```scala mdoc
colString(c"name_user")
colInt(c"age_user")
colString(c"city_user")
//and many more
```
So we can make a function to simplify it:
```scala mdoc
import doric.types.SparkType
def user[T: SparkType](colName: CName): DoricColumn[T] = {
  col[T](colName + "_user")
}
```
In valid cases it works ok, bug when an error is produce because one of these references, it will point to the line `col[T](colName + "_user")` that is not the real problem.

```scala mdoc:crash
val userc = user[Int](c"name") //wrong type :S
userDF.select(userc)
```

What we really want is to mark as the source the place we are using our `user` method. We can achieve this by adding only an implicit value to the definition:
```scala mdoc:invisible:reset
val spark = doric.DocInit.getSpark
      
import spark.implicits._

val userDF = List(
("Foo", "Madrid", 35),
("Bar", "New York", 40),
("John", "Paris", 30)
).toDF("name_user", "city_user", "age_user")

```

```scala mdoc
import doric._
import doric.sem.Location
import doric.types.SparkType

def user[T: SparkType](colName: CName)(implicit location: Location): DoricColumn[T] = {
  col[T](colName + "_user")
}
```
Now if we repeat the same error we will be pointed to the real problem
```scala mdoc:crash
val age = user[Int](c"name")
val team = user[String](c"team")
userDF.select(age, team)
```
