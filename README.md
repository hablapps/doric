# faisca

This library makes it easier to write Spark code:

* it exposes the Spark SQL functions that don't exist in the Scala API
* exposes a minimalistic functions API without overloaded method signatures
* allows for type safe programming and compile-time errors
* allows for syntactic sugar for common functions

Fun fact: faísca means Spark in Portuguese.

## functionsf

`functionsf` stands for "functions faisca".

There are some Spark SQL functions that the maintainers don't want to expose via Scala.

These are native Spark functions, so they're performant.

## functionsb

`functionsb` stands for "functions better"

The `org.apache.spark.sql.functions` can be a bit difficult to work with:

* Lots of overloaded methods
* Some functions take Scala objects as arguments instead of `Column` objects
* The inconsistent use of camelCase, snake_case, and alllowercase
* Lots of overloaded methods
* Inconsistent parameter names

## Type safe programming

Spark doesn't allow for type safe programming, not even the Dataset API.

### Runtime error

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

Here's how we can run the same code with `columnsf` defined in this faisca library:

```scala
df.withColumn("hour", "some_int".ic.hour)
```

Our code won't compile and will give us this descriptive error:

```
[error] /Users/powers/Documents/code/my_apps/faisca/src/test/scala/mrpowers/faisca/ColumnsFSpec.scala:62:51: value hour is not a member of mrpowers.faisca.IntegerColumn
[error]     val res = df.withColumn("hour", "some_int".ic.hour)
[error]                                                   ^
```

Our text editor will also complain:

![compile-time error](https://github.com/MrPowers/faisca/blob/master/images/compile_time_error.png)

### No errors

Other times, nonsensical operations just return `null` without throwing an error.



## Syntactic sugar

Syntactic sugar should normally be avoided except in cases where it can make the code significantly more readable.

<<ADD example with lots of lit() and Date.valueOf()>>
