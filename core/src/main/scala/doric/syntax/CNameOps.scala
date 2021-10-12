package doric
package syntax

import doric.sem.Location
import doric.types.SparkType

trait CNameOps {

  implicit class StringCNameOps(s: String) {
    @inline def cname: CName = CName(s)
  }

  implicit class CNameOps(colName: CName) {
    @inline def apply[T: SparkType](implicit
        location: Location
    ): DoricColumn[T] =
      col[T](colName)
  }

}
