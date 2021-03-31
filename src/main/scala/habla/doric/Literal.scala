package habla.doric

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import scala.annotation.implicitNotFound

@implicitNotFound("The type $ScalaType cant be casted as a literal for $T.")
trait Literal[T, ScalaType] {
  def createLiteral(st: ScalaType): Column = lit(st)

  def createTLiteral(st: ScalaType): DoricColumn[T] =
    DoricColumn(createLiteral(st))
}

object Literal {
  @inline def apply[T, ScalaType](implicit imp: Literal[T, ScalaType]): Literal[T, ScalaType] = {
    imp
  }
}
