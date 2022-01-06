package doric

import doric.types.SparkType
import monix.newtypes.NewtypeWrapped

object CName extends NewtypeWrapped[String] {
  implicit def columnByName[T: SparkType](colName: CName): NamedDoricColumn[T] =
    col(colName.value)
}
