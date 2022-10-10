package doric
package syntax

import doric.sem.Location
import doric.types.SparkType.Primitive
import doric.types.{LiteralSparkType, SparkType}

private[syntax] trait LiteralConversions {

  /**
    * Creates a literal with the provided value.
    *
    * @param litv
    * the element to create as a literal.
    * @tparam L
    * The type of the literal.
    * @return
    * A doric column that represent the literal value and the same type as the
    * value.
    */
  def lit[L: SparkType: LiteralSparkType](
      litv: L
  )(implicit l: Location): LiteralDoricColumn[L] = {
    LiteralDoricColumn(litv)
  }

  implicit class LiteralOps[L: SparkType: LiteralSparkType](litv: L) {

    /**
      * Transforms the original value to a literal.
      *
      * @return
      * a literal with the same type.
      */
    @inline
    def lit(implicit l: Location): LiteralDoricColumn[L] = LiteralDoricColumn(
      litv
    )
  }

  implicit class DoricColLiteralGetter[T: Primitive](dCol: DoricColumn[T]) {
    @inline def getValueIfLiteral: Option[T] = dCol match {
      case literal: LiteralDoricColumn[T] =>
        Some(literal.columnValue)
      case _ => None
    }
  }

}
