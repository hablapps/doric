---
title: Doric Documentation
permalink: docs/modularity/
---

```scala mdoc:invisible
import org.apache.spark.sql.{Column, functions => f}

val spark = doric.DocInit.getSpark
import spark.implicits._

import doric._
import doric.implicitConversions._
```

# Fostering modularity

Modular programming with Spark `Column` expressions is simply not possible. For instance, let's try to solve a simple
problem in a modular way. We start from the following `DataFrame`, whose columns finish with the same suffix "_user":

```scala mdoc:invisible
val userDF = List(
("Foo", "Madrid", 35),
("Bar", "New York", 40),
("John", "Paris", 30)
).toDF("name_user", "city_user", "age_user")
```

```scala mdoc
userDF.show()
userDF.printSchema()
```

We may refer to these columns using their full names: 

```scala mdoc
f.col("name_user")
f.col("age_user")
f.col("city_user")
//and many more
```

But we may also want to create a _reusable_ function which abstract away the common suffix and allows us to focus
on the unique part of the column names (thus obtaining a more modular solution):

```scala mdoc
def userCol(colName: String): Column =
  f.col(colName + "_user")
```

If we refer to an existing prefix, everything is fine. But, if we refer to a non-existing column in a `select` 
statement, for instance, then we would like to know where exactly that reference was made. The problem is that Spark 
will refer us to the line of the `select` statement instead: 

```scala mdoc:crash
val userc = userCol("name1") // actual location of error :S
userDF.select(userc)        // error location reported by Spark
```

### Towards the source of error

Doric includes in the exception the exact line of the malformed column reference (cf. [error reporting in doric](errors.md)),
and this will be of great help in solving our problem. However, the following doric function doesn't really work:

```scala mdoc
import doric.types.SparkType
def user[T: SparkType](colName: String): DoricColumn[T] = {
  col[T](colName + "_user")
}
```

Indeed, the following code will point out to the implementation of the `user` function: 

```scala mdoc:crash
val age = user[Int]("name")
val team = user[String]("team")
userDF.select(age, team)
```


To get the right line of code in the reported error, we can just add the following implicit parameter to the `user`
function: 

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

def user[T: SparkType](colName: String)(implicit location: Location): DoricColumn[T] = {
  col[T](colName + "_user")
}
```

Now, if we repeat the same error we will be referred to the real problem:

```scala mdoc:crash
val age = user[Int]("name")
val team = user[String]("team")
userDF.select(age, team)
```

