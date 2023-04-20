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
//   Kleisli(cats.data.Kleisli$$Lambda$2909/0x00000001012ed840@3d72abea)
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
