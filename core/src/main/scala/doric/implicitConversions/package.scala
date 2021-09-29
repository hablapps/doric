package doric

import doric.types.{Casting, SparkType}

package object implicitConversions {
  implicit def literalConversion[L: SparkType](litv: L): DoricColumn[L] = {
    litv.lit
  }

  implicit def implicitSafeCast[F: SparkType, T: SparkType](
      fromCol: DoricColumn[F]
  )(implicit cast: Casting[F, T]): DoricColumn[T] =
    cast.cast(fromCol)

  implicit def stringCname(colName: String): CName = CName(colName)
}
