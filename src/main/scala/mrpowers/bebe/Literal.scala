package mrpowers.bebe

import scala.annotation.implicitNotFound

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

@implicitNotFound("The type $ScalaType cant be casted as a literal for $T.")
trait Literal[T, ScalaType] {
  def createLiteral(st: ScalaType): Column = lit(st)

  def createTLiteral(st: ScalaType)(implicit fromDf: FromDf[T]): T =
    fromDf.construct(createLiteral(st))
}
