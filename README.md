# bebé

This library makes it easier to write Spark code:

* provides access to the Spark SQL functions that aren't exposed in the Scala API (e.g. `regexp_extract_all`)
* allows for type safe Spark programming
* provides syntactic sugar for clean Spark code

![Anuel](https://github.com/MrPowers/bebe/blob/main/images/anuel.jpg)

## MissingFunctions

There are some Spark SQL functions that the maintainers don't want to expose via Scala.  For example, the Spark maintainers [intentionally removed regexp_extract_all](https://github.com/apache/spark/pull/31306#issuecomment-766466106) from the Scala API.

This package provides easy Scala access to functions that are already implemented in SQL.

Let's extract all the numbers from the `some_string` column in the following DataFrame: 

```
+----------------------+
|some_string           |
+----------------------+
|this 23 has 44 numbers|
|no numbers            |
|null                  |
+----------------------+
```

Use `bebe_regexp_extract_all` to create an `ArrayType` column with all the numbers from `some_string`:

```scala
import org.apache.spark.sql.BebeFunctions._

df
  .withColumn("actual", bebe_regexp_extract_all(col("some_string"), lit("(\\d+)"), lit(1)))
  .show(false)
```

```
+----------------------+--------+
|some_string           |actual  |
+----------------------+--------+
|this 23 has 44 numbers|[23, 44]|
|no numbers            |[]      |
|null                  |null    |
+----------------------+--------+
```

The `BebeFunctions` are prefixed with `bebe` to avoid a name conflict in the unlikely event that `regexp_extract_all` is added to `org.apache.spark.sql.fucntions` at some point in the future.

## Syntax sugar

All the parens from `lit()` and `col()` make this code hard to read.  Let's import the extensions and reformat the code:

```scala
import mrpowers.bebe.Extensions._

df
  .withColumn("actual", bebe_regexp_extract_all("some_string".c, "(\\d+)".l, 1.l))
  .show()
```

## TypedFunctions

The `TypedFunctions` rely on objects like `IntegerColumn`, `DateColumn`, and `TimestampColumn` rather than generic `Column` objects.

Most of the Spark functions should only be run on certain column types.  `hour()` should only be run on `TimestampType` columns.  It doesn't make any sense to run `hour()` on a `IntegerType` column.

Nonsensical Spark code sometimes throws runtime errors and other times returns `null`.

It's better to throw errors at compile-time.  Runtime errors allow you to compile invalid JARs and deploy jobs that error out in production.

Compile-time errors prevent you from compiling invalid JARs and releasing buggy code in production.

Here's the `org.apache.spark.sql.add_months` type signature:

```scala
def add_months(startDate: Column, numMonths: Column): Column
```

It takes generic column objects as arguments and returns a generic column object.  This isn't desirable because `add_months` only makes sense for `DateType` and `TimestampType` columns.

Here are the `org.apache.spark.sql.TypedFunctions` definitions from this library:

```scala
def add_months(startDate: DateColumn, numMonths: IntegerColumn): DateColumn

def add_months(startTime: TimestampColumn, numMonths: IntegerColumn): TimestampColumn
```

These type signatures have the following advantages:

* You can't pass them a nonsensical argument (e.g. you'll get a compile-time error if you try to run the function with an `IntegerColumn`)
* The return type is clear (some functions like `last_day()` will surprise you with a `DateType` return value when they're run on a `TimestampType` column - this interface eliminates surprises)

Keep reading for more details on Spark function limitations and an even better way to write type safe code.

## Spark function limitations

The `org.apache.spark.sql.functions` can be difficult to work with:

* Lots of overloaded methods
* Some functions take Scala objects as arguments instead of `Column` objects
* `Column` objects are basically untyped, so difficult to write type safe code
* The inconsistent use of camelCase, snake_case, and lowercase names that don't use underscores
* Inconsistent parameter names

Let's look at the different types of runtime errors you'll encounter when working with the `org.apache.spark.sql.functions`.

### Some nonsensical operations are caught at runtime

Create a Dataset with an integer column and try to add four months to the integer.

```scala
case class Cat(name: String, favorite_number: Int)

val catsDS = Seq(
  Cat("fluffy", 45)
).toDS()

catsDS.withColumn("meaningless", add_months($"favorite_number", 4)).show()
```

Here's the error message: org.apache.spark.sql.AnalysisException: cannot resolve 'add_months(`favorite_number`, 4)' due to data type mismatch: argument 1 requires date type, however, '`favorite_number`' is of int type.;;

AnalysisExceptions are thrown at runtime, so this isn't a compile-time error that you'd expect from a type safe API.

### Other nonsensical operations return null

Let's run the `date_trunc` function on a `StringType` column and observe the result.

```scala
catsDS.withColumn("meaningless", date_trunc("name", lit("cat"))).show()
```

```
+------+---------------+-----------+
|  name|favorite_number|meaningless|
+------+---------------+-----------+
|fluffy|             45|       null|
+------+---------------+-----------+
```

Some Spark functions just return `null` when the operation is meaningless.  `lit("cat")` isn't a valid format, so this operation will always return `null`.

### Some operations return meaningless results

Let's create a Dataset with a date column and then reverse the date:

```scala
import java.sql.Date

case class Birth(hospitalName: String, birthDate: Date)

val birthsDS = Seq(
  Birth("westchester", Date.valueOf("2014-01-15"))
).toDS()

birthsDS.withColumn("meaningless", reverse($"birthDate")).show()
```

```
+------------+----------+-----------+
|hospitalName| birthDate|meaningless|
+------------+----------+-----------+
| westchester|2014-01-15| 51-10-4102|
+------------+----------+-----------+
```

At the very least, we'd expect this code to error out at runtime with a `org.apache.spark.sql.AnalysisException`.

A type-safe implementation would throw a compile time error when the `reverse` function is passed a `DateType` value.

## Type safe programming

Let's demonstrate how `bebe` allows for type safe programming.

### Compile-time errors

Some nonsensical Spark computations throw errors at runtime (instead of at compile time as would be expected in a type safe framework).

Suppose you have the following DataFrame:

```
+--------+
|some_int|
+--------+
|       1|
|       2|
|       3|
+--------+
```

Run the `org.apache.spark.sql.functions.hour` function on the `some_int` column:

```scala
df
  .withColumn("hour", hour(col("some_int")))
  .show()
```

Here's the error message:

```
org.apache.spark.sql.AnalysisException: cannot resolve 'hour(`some_int`)' due to data type mismatch: argument 1 requires timestamp type, however, '`some_int`' is of int type.;
```

This is a runtime error.  That's not what we want.  We'd rather get a compile-time error.

Here's how we can run the same code with `TypedFunctions` defined in this library:

```scala
df.withColumn("hour", "some_int".ic.hour)
```

Our code won't compile and will give us this descriptive error:

```
[error] /Users/powers/Documents/code/my_apps/bebe/src/test/scala/mrpowers/bebe/ColumnsFSpec.scala:62:51: value hour is not a member of mrpowers.bebe.IntegerColumn
[error]     val res = df.withColumn("hour", "some_int".ic.hour)
[error]                                                   ^
```

Our text editor will also complain:

![compile-time error](https://github.com/MrPowers/bebe/blob/main/images/compile_time_error.png)

### No errors

Other times, nonsensical operations just return `null` without throwing an error.



## Syntactic sugar

Syntactic sugar should normally be avoided except in cases where it can make the code significantly more readable.

<<ADD example with lots of lit() and Date.valueOf()>>
