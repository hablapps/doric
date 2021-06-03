# [doric](https://en.wikipedia.org/wiki/Doric_order)

## A type of column for typing columns!!!

[![CI](https://github.com/hablapps/doric/actions/workflows/ci.yml/badge.svg)](https://github.com/hablapps/doric/actions/workflows/ci.yml)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/hablapps/doric/HEAD?filepath=Doric%20Infomercial.ipynb)

This library makes it easier to write Spark code:

* allows for type safe Spark programming
* provides syntactic sugar for clean Spark code

## Installation

Fetch the JAR from Maven:

```scala
libraryDependencies += "org.hablapps" %% "doric" % "0.0.1"
```

`Doric` depends on Spark internals, and it's been tested against the following spark versions.

| Spark | Scala | doric  |
|-------|-------|-------|
| 3.1.0 | 2.12  | 0.0.1 |

## Typed columns

Spark is a weak typed framework created with a strong type langage as scala. It's true that spark cant be sure about the type of the of the readed columns, but we, as developer can at least expect a type.
Doric is a thin layer above the spark Column class, that can validate in runtime the type when we ask for a column in the dataframe. Once done this, we can use this columns in a typesafe enviroment preventing invalid functions, and having compile time exceptions of possible problems.

How can be use this? Just type in the class you need:
```scala
import habla.doric
```

To extract a column and validate if it's a IntegerColumn?
```scala
val c: IntegerColumn = df.get[IntegerColumn]()
```
Need a spark column?
```scala
val sc: Column = c.sparkColumn
```
No magic or relearn spark again

Types you can use at this moment?
Numeric:
* IntegerColumn
* LongColumn
* FloatColumn
* DoubleColumn
Time:
* DateColumn
* TimestampColumn

Others:
* StringColumn
* BooleanColumn

This is going to make me write more code?
Not much, just specify the type of the column, but the rest is almost the same as your spark everyday job.

```scala
val df: Dataframe = ???

val df2 = df.withColumn("newCol", df.get[IntegerColumn]("c1") + df.get[IntegerColumn]("c2"))
```
`c1` or c`2 doesn't exist? You get your normal spark error that the column is not above the founded columns, but if "c1" is not ingeter, you get a runtime error that the column type was not the expected.
