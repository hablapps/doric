package doric
package syntax

import doric.sem.Location
import doric.types.SparkType

trait CNameOps {

  implicit class StringCNameOps(s: String) {
    @inline final def cname: CName = CName(s)
  }

  implicit class CNameOps(colName: CName) {
    @inline final def apply[T: SparkType](implicit
        location: Location
    ): DoricColumn[T] =
      col[T](colName.value)

    final def concat(sufix: CName): CName =
      CName(colName.value + sufix.value)

    final def /(child: CName): CName =
      CName(colName.value + "." + child.value)

    final def +(sufix: String): CName =
      CName(colName.value + sufix)
  }

}
