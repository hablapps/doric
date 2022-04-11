package doric

import doric.types.SparkType

case class CName(value: String)

object CName {
  implicit def columnByName[T: SparkType](colName: CName): NamedDoricColumn[T] =
    col(colName.value)
}
