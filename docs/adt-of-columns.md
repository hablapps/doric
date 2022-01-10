---
title: Doric Documentation
permalink: docs/adt-columns/
---
# Doric column ADT hierarchy
Doric column is an algebraic data type (ADT) that represent the validations and transformations that are needed to obtain the spark expression required.
Doric column has two subtypes of column, that can provide more info about the content of the column.

## TransformationDoricColumn
It's the most basic type of column, that only carries with the needed info about transformations.

## NamedDoricColumn
A column with a defined name. You can obtain this kind of columns in two ways, referring a dataframe column, with `col[String](c"colname")` for example, or assigning an alias/as to a transformed column `mycolumn.as(c"mycol")`.
This way Doric helps you to always carrie the name that was assigned and allows you to extract it.

```scala
import doric._

val colFromDF: NamedDoricColumn[String] = col[String](c"user")
// colFromDF: NamedDoricColumn[String] = NamedDoricColumn(
//   Kleisli(doric.types.SparkType$$Lambda$1489/539123635@7f2b39a),
//   "user"
// )
val colWithAlias: NamedDoricColumn[Int] = col[Int](c"int1") + col[Int](c"int2") as c"newVal"
// colWithAlias: NamedDoricColumn[Int] = NamedDoricColumn(
//   Kleisli(cats.data.Kleisli$$Lambda$1493/514600028@799f354a),
//   "newVal"
// )

println(colFromDF.columnName)
// user
println(colWithAlias.columnName)
// newVal
```
