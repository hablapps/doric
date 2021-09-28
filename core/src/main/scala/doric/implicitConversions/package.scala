package doric

import doric.types.{Casting, LiteralSparkType, SparkType}

package object implicitConversions {
  implicit def literalConversion[L: SparkType: LiteralSparkType](
      litv: L
  ): LiteralDoricColumn[L] =
    litv.lit

  implicit def literaConversionSafeCast[
      L: SparkType: LiteralSparkType,
      LO: SparkType
  ](litv: L)(implicit
      cast: Casting[L, LO]
  ): DoricColumn[LO] =
    litv.lit.cast[LO]

  implicit def implicitSafeCast[F: SparkType, T: SparkType](
      fromCol: DoricColumn[F]
  )(implicit cast: Casting[F, T]): DoricColumn[T] =
    cast.cast(fromCol)

  implicit def stringCname(colName: String): CName =
    CName(colName)

}
