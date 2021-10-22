---
title: Typed columns in doric
permalink: docs/typed/
---
Doric provides out of the box some basic types that have a correspondence in dataframes.


|Scala / doric type | Spark type|
|-------------------|-----------|
|Boolean|BooleanType|
|Int|IntegerType|
|Long|LongType|
|Float|FloatType|
|Double|DoubleType|
|String| StringType|
|java.sql.Timestamp| TimestampType|
|java.time.Instant|TimestampType|
|java.sql.Date|DateType|
|java.time.LocalDate|DateType|
|doric.Dstruct|StructType|
|Array[T]|ArrayType|
|Map[K,V]|MapType|

With these types you will always track the type of your columns and know what transformations are allowed in each moment.
To know all possible transformations, you can take a look at the [DoricColumn API doc](docs/api/latest/doric/DoricColumn.html).
