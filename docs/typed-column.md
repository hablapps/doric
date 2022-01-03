---
title: Typed columns in doric
permalink: docs/typed/
---
Doric provides out of the box some basic types that have a correspondence in dataframes.


|Scala / doric type | Spark type| Information |
|-------------------|-----------|---|
|Boolean|BooleanType|
|Int|IntegerType|
|Long|LongType|
|Float|FloatType|
|Double|DoubleType|
|String| StringType|
|java.sql.Timestamp| TimestampType| Custom type of Instant|
|java.time.Instant|TimestampType|
|java.sql.Date|DateType| Custom type of LocalDate|
|java.time.LocalDate|DateType|
|org.apache.spark.sql.Row|StructType|Represents a struct|
|Array[T]|ArrayType|
|List[T]|ArrayType|
|Map[K,V]|MapType|
|Option[T]| |Wrapper to make easier to handle nullable values|

With these types you will always track the type of your columns and know what transformations are allowed in each moment.
To know all possible transformations, you can take a look at the [DoricColumn API doc](/docs/api/latest/doric/DoricColumn.html).
Timestamp and Date are custom types derived from the most modern API of time manipulation spark can handle.
