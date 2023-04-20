---
title: Doric Documentation
permalink: docs/doric-exclusive-features/
---

# Doric exclusive features


## Aggregations with doric syntax

Doric introduces a simpler way to implement user defined aggregations, using doric's own syntax.
The creation needs only five elements:

- The column to make the aggregation
- zero: the initialization of the value on each task.
- Update: the function to update the accumulated value when processing a new column value.
- Merge: a function to compare the results of all the Update results, two by two
- The final transformation of the value once all merges are applied.

The following example shows how to implement the average of a column of type int

```scala
      val customMean = customAgg[Long, Row, Double](
        col[Long]("id"), // The column to work on
        struct(lit(0L), lit(0L)), // setting the sum and the count to 0
        (x, y) =>
          struct(
            x.getChild[Long]("col1") + y, // adding the value to the sum
            x.getChild[Long]("col2") + 1L.lit // increasing in 1 the count of elements
          ),
        (x, y) =>
          struct(
            x.getChild[Long]("col1") + y.getChild[Long]("col1"), // obtaining the total sum of all 
            x.getChild[Long]("col2") + y.getChild[Long]("col2") // obtaining the total count of all
          ),
        x => x.getChild[Long]("col1") / x.getChild[Long]("col2") // the total sum divided by the count
      )
```

Now you can use your new aggregation as usual

```scala
spark.range(10).show()
// +---+
// | id|
// +---+
// |  0|
// |  1|
// |  2|
// |  3|
// |  4|
// |  5|
// |  6|
// |  7|
// |  8|
// |  9|
// +---+
// 

spark.range(10).select(customMean.as("customMean")).show()
// +----------+
// |customMean|
// +----------+
// |       4.5|
// +----------+
//
```

## Custom sort for array columns & structured array columns
Maybe you had to sort arrays more than a couple of times, not a big deal, but maybe you had to sort arrays of structs... Now it gets interesting.

Currently, (spark 3.3.2 is the latest version) there is only one API function to sort arrays called `array_sort`, this will sort in descendant order any array type (in case of structs it will sort taking account the first column, then the second and so on). If you want to perform some custom order you have to write your own "spark function" or create an expression via SQL using which allows you to use the lambda function.

Doric provides this function for earlier versions (since spark 3.0). In fact, doric provides a simplified functions in case you need to order a structured array column just providing the sub-column names instead of creating an ad-hoc order function

```scala
case class Character(name: String, description: String, age: Int)
org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)

val dfArrayStruct = Seq(
  Seq(
    Character("Terminator", "T-800", 80),
    Character("Yoda", null, 900),
    Character("Gandalf", "white", 2),
    Character("Terminator", "T-1000", 1),
    Character("Gandalf", "grey", 2000)
  )
).toDF
// dfArrayStruct: org.apache.spark.sql.package.DataFrame = [value: array<struct<name:string,description:string,age:int>>]

val sparkCol = f.expr("array_sort(value, (l, r) -> case " +
  // name ASC
  "when l.name < r.name then -1 " +
  "when l.name > r.name then 1 " +
  "else ( case" +
  // age DESC
  "  when l.age > r.age then -1 " +
  "  when l.age < r.age then 1 " +
  "  else 0 end " +
  ") end)"
)
// sparkCol: org.apache.spark.sql.Column = array_sort(value, lambdafunction(CASE WHEN (l.name < r.name) THEN -1 WHEN (l.name > r.name) THEN 1 ELSE CASE WHEN (l.age > r.age) THEN -1 WHEN (l.age < r.age) THEN 1 ELSE 0 END END, l, r))

val doricCol = colArray[Row]("value").sortBy(CName("name"), CNameOrd("age", Desc))
// doricCol: ArrayColumn[Row] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$2909/0x00000001012ed840@3bb29351)
// )

dfArrayStruct.select(sparkCol.as("sorted")).show(false)
// +-----------------------------------------------------------------------------------------------------------------+
// |sorted                                                                                                           |
// +-----------------------------------------------------------------------------------------------------------------+
// |[{Gandalf, grey, 2000}, {Gandalf, white, 2}, {Terminator, T-800, 80}, {Terminator, T-1000, 1}, {Yoda, null, 900}]|
// +-----------------------------------------------------------------------------------------------------------------+
// 
dfArrayStruct.select(doricCol.as("sorted")).show(false)
// +-----------------------------------------------------------------------------------------------------------------+
// |sorted                                                                                                           |
// +-----------------------------------------------------------------------------------------------------------------+
// |[{Gandalf, grey, 2000}, {Gandalf, white, 2}, {Terminator, T-800, 80}, {Terminator, T-1000, 1}, {Yoda, null, 900}]|
// +-----------------------------------------------------------------------------------------------------------------+
//
```
## Column mappings/matches

Sometimes we must perform a mapping transformation based on a column value, so if its value is `key1` the output must
be `result1`, if the value is `key2` the output must be `result2`, and so on.

This is usually achieved by a `when` series using **spark**.

```scala
val dfMatch = Seq("key1", "key2", "key3", "anotherKey1", "anotherKey2").toDF()
// dfMatch: org.apache.spark.sql.package.DataFrame = [value: string]

val mapColSpark = f.when(f.col("value") === "key1", "result1")
  .when(f.col("value") === "key2", "result2") // actually we could write here a different column name, so the when will not work properly
  .when(f.length(f.col("value")) > 4, "error key")
  .otherwise(null)
// mapColSpark: org.apache.spark.sql.Column = CASE WHEN (value = key1) THEN result1 WHEN (value = key2) THEN result2 WHEN (length(value) > 4) THEN error key ELSE NULL END
```

We haven't reinvented the wheel, but now it is fail-proof (as we always match to the same column) and much simpler to
map values using **doric**:

```scala
val mapColDoric = colString("value").matches[String]
  // simple mappings, it is the same as if we use _ === "whatever"
  .caseW("key1".lit, "result1".lit)
  .caseW("key2".lit, "result2".lit)
  // function equality
  .caseW(_.length > 4, "error key".lit)
  .otherwiseNull
// mapColDoric: DoricColumn[String] = TransformationDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$2909/0x00000001012ed840@f0a6b04)
// )

dfMatch.withColumn("mapResult", mapColDoric).show()
// +-----------+---------+
// |      value|mapResult|
// +-----------+---------+
// |       key1|  result1|
// |       key2|  result2|
// |       key3|     null|
// |anotherKey1|error key|
// |anotherKey2|error key|
// +-----------+---------+
//
```

It is also a lot easier if you have a list of transformations, as we use the doric when builder under the hoods:
```scala
val transformations = Map(
  "key1" -> "result1",
  "key2" -> "result2",
  "key4" -> "result4"
)

// spark
val sparkFold = transformations.tail.foldLeft(f.when(f.col("value") === transformations.head._1, transformations.head._2)) {
  case (cases, (key, value)) =>
    cases.when(f.col("value") === key, value)   // once again, what if I make a mistake and I write a different column?
}
  
sparkFold.otherwise(null)

// doric
val doricFold = transformations.foldLeft(colString("value").matches[String]) {
  case (cases, (key, value)) =>
    cases.caseW(key.lit, value.lit)
}
  
doricFold.otherwiseNull
```

## Array zipWithIndex function
How many times have you need `zipWithIndex` scala function in spark? Not many, probably, but if you have to do it now Doric helps you out!:
```scala
val dfArray = List(
  Array("a", "b", "c", "d"),
  Array.empty[String],
  null
).toDF("col1")
  .select(colArrayString("col1").zipWithIndex().as("zipWithIndex"))
// dfArray: org.apache.spark.sql.package.DataFrame = [zipWithIndex: array<struct<index:int,value:string>>]

dfArray.printSchema()
// root
//  |-- zipWithIndex: array (nullable = true)
//  |    |-- element: struct (containsNull = false)
//  |    |    |-- index: integer (nullable = false)
//  |    |    |-- value: string (nullable = true)
// 

dfArray.show(false)
// +--------------------------------+
// |zipWithIndex                    |
// +--------------------------------+
// |[{0, a}, {1, b}, {2, c}, {3, d}]|
// |[]                              |
// |null                            |
// +--------------------------------+
//
```

## Map toArray function
Doric also provides a function to "cast" a map into an array. We have done nothing fancy, but it might help with some use cases.
```scala
val dfMap = List(
  ("1", Map("a" -> "b", "c" -> "d")),
  ("2", Map.empty[String, String]),
  ("3", null)
).toDF("ix", "col")
  .select(colMapString[String]("col").toArray.as("map2Array"))
// dfMap: org.apache.spark.sql.package.DataFrame = [map2Array: array<struct<key:string,value:string>>]

dfMap.printSchema()
// root
//  |-- map2Array: array (nullable = true)
//  |    |-- element: struct (containsNull = false)
//  |    |    |-- key: string (nullable = true)
//  |    |    |-- value: string (nullable = true)
// 

dfMap.show(false)
// +----------------+
// |map2Array       |
// +----------------+
// |[{a, b}, {c, d}]|
// |[]              |
// |null            |
// +----------------+
//
```
