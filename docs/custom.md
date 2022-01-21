---
title: Custom types in doric
permalink: docs/custom/
---


# Custom types

The limitation of the sql nature of Spark SQL limits the amount of types it contains. Doric tries to make easier to
connect the scala API of Spark with any other element you need in scala. The sparkType typeclass is the one in charge to
show spark how your custom types are represented, and how can we extract it from the DataFrame. Also, to make it easier
to use as literals your custom types, doric has the typeclass LiteralSparkType, that is in charge of transforming the
literal value to the spark representation.

Now we show a few useful examples to learn how to create our custom types in doric.

## User as a string

Let's think a field with the name and the surname of a user, we want to represent it outside of spark as a case class:

```scala
case class User(name: String, surname: String)
```

But we don't want to represent it in spark as a struct, it will be better to represent it as a simple string. In doric
this is super simple, just type this code:

```scala
import doric.types.{SparkType, LiteralSparkType}

implicit val userSparkType = SparkType[String].customType[User](
        x => {
          val name :: surname :: Nil = x.split("#").toList
          User(name, surname)
        }
      )
// userSparkType: SparkType[User]{type OriginalSparkType = String} = doric.types.SparkType$$anon$1@3c995eae
      
implicit val userLiteralSparkType =
  LiteralSparkType[String].customType[User](x => s"${x.name}#${x.surname}")
// userLiteralSparkType: LiteralSparkType[User]{type OriginalSparkType = String} = doric.types.LiteralSparkType$$anon$1@54fb9c08
```

Let's take a closer look, first we are creating an implicit `SparkType` for `User`. And the way to do this is invoking
the SparkType of the original datatype we want to use, in our case calling `SparkType[String]`. Once we have it, we can
call the method `customType`. This method needs the function that will transform from `String` to our custom `User`,
split the String by the character `#`  and reconstruct the `User` class. Also, to allow to use the `User` class as a
literal value, we need to create a LiteralSparkType, that starting with the original type that we created it, we call
the method `customType` and passing the type of our `User`
and we have to provide the opposite function to the one for the `SparkType`, in our case how to create the `String` from
our `User`.

Now we have a valid SparkType, we can use it for everything:

* use it as a literal

```scala
df.withColumn("user", User("John", "Doe").lit).show()
// +--------+
// |    user|
// +--------+
// |John#Doe|
// +--------+
//
```

* select a column with type `User`

```scala
import doric.implicitConversions.literalConversion
df.withColumn("expectedUser", col[User]("user") === User("John", "Doe"))
// res2: org.apache.spark.sql.package.DataFrame = [user: string, expectedUser: boolean]
```

* collect the column and obtain a `User` in your driver

```scala
println(df.collectCols(col[User]("user")))
// List(User(Jane,Doe))
```

We have to always keep in mind that inside our dataframe, the user is represented as a String:

```scala
df.select(User("John", "Doe").lit.as("user")).printSchema
// root
//  |-- user: string (nullable = false)
//
```

So can be a good idea to create a casting to the string value, in this case spark will do nothing because it's already
a `String` inside the dataframe.

```scala
import doric.types.SparkCasting
implicit val userStringCast = SparkCasting[User, String]
// userStringCast: types.Casting[User, String] = doric.types.SparkCasting$$anon$1@6dd729
```

But the real power of this custom types is the ability to create also custom functions for the `DoricColumn[User]`

```scala
implicit class DoricUserMethods(u: DoricColumn[User]) {
  def name: StringColumn = u.cast[String].split("#").getIndex(0)
  def surname: StringColumn = u.cast[String].split("#").getIndex(1)
}

df.filter(col[User]("user").name === "John")
// res5: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [user: string]
```

The power for a much reusable and descriptive code at your service.

## Other example

A simple case, give context to a numeric value that represent the state of a person. In our data we have a column that
represents if the user is single(1), in a relation(2) or married(3), but inside the dataframe is the number. We would
first create our classes:

```scala
sealed trait UserState
object Single extends UserState
object Relation extends UserState
object Married extends UserState
```

Now we can create our `SparkType[UserState]` using `Int` as our base `DataType`. Also the `LiteralSparkType[UserState]`

```scala
val stateFromSpark: Int => UserState = {
  case 1 => Single
  case 2 => Relation
  case 3 => Married
  case _ => throw new Exception("not a valid value")
}
// stateFromSpark: Int => UserState = <function1>

val stateToSpark: UserState => Int = {
  case Single => 1
  case Relation => 2
  case Married => 3
}
// stateToSpark: UserState => Int = <function1>

implicit val userStateSparkType = SparkType[Int].customType(stateFromSpark)
// userStateSparkType: SparkType[UserState]{type OriginalSparkType = Int} = doric.types.SparkType$$anon$1@767785a4
implicit val userLiteralStateSparkType = LiteralSparkType[Int].customType(stateToSpark)
// userLiteralStateSparkType: LiteralSparkType[UserState]{type OriginalSparkType = Int} = doric.types.LiteralSparkType$$anon$1@7ee45cbe
```

Now let's do some complex logic, increase a score depending on the state of the user.

```scala
val changeScore: IntegerColumn = when[Int]
  .caseW(col[UserState]("state") === Single, col[Int]("score") * 2)
  .caseW(col[UserState]("state") === Relation, col[Int]("score") * 10)
  .otherwise(col[Int]("score") * 12)
// changeScore: IntegerColumn = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$1475/2107284848@5634f627)
// )
```

This way we can make our code closer to scala syntax, but with all the power of Spark.

```scala
import spark.implicits._
List(
  ("User1#Surname1", 1, 5),
  ("User2#Surname2", 2, 5),
  ("User3#Surname2", 3, 5)
).toDF("user", "state", "score")
  .withColumn("newScore", changeScore)
  .show()
// +--------------+-----+-----+--------+
// |          user|state|score|newScore|
// +--------------+-----+-----+--------+
// |User1#Surname1|    1|    5|      10|
// |User2#Surname2|    2|    5|      50|
// |User3#Surname2|    3|    5|      60|
// +--------------+-----+-----+--------+
//
```

## Custom types with type parameters

Doric not only allows to create simple types, it can create complex types like `Set`, represented in spark as a `List`
or `Array`. We will need to know that our type inside of spark will still be an array, with the main difference that if
we insert something in our column it can be repeated. This is as simple as create the following lines:

```scala
implicit def setSparkType[T: SparkType] =
  SparkType[List[T]].customType[Set[T]](_.toSet)
  
implicit def setLiteralSparkType[T: LiteralSparkType](implicit lst: SparkType[Set[T]]) =
  LiteralSparkType[List[T]].customType[Set[T]](_.toList)
```

All set up, let's enjoy our new type

```scala
val dfWithSet = df.select(Set("a", "b", "a", "c", "b").lit.as("mysetInSpark"))
// dfWithSet: org.apache.spark.sql.package.DataFrame = [mysetInSpark: array<string>]
dfWithSet.show
// +------------+
// |mysetInSpark|
// +------------+
// |   [a, b, c]|
// +------------+
// 
println(dfWithSet.collectCols(col[Set[String]]("mysetInSpark")).head)
// Set(a, b, c)
```
