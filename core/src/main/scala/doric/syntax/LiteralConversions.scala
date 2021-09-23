package doric
package syntax

import cats.implicits.catsSyntaxApplicativeId
import doric.types.SparkType

import org.apache.spark.sql.functions

private[syntax] trait LiteralConversions {

  /**
    * Creates a literal with the provided value.
    * @param litv
    *   the element to create as a literal.
    * @tparam L
    *   The type of the literal.
    * @return
    *   A doric column that represent the literal value and the same type as the
    *   value.
    */
  def lit[L: SparkType](litv: L): DoricColumn[L] =
    functions.lit(litv).pure[Doric].toDC

  implicit class LiteralOps[L: SparkType](litv: L) {

    /**
      * Transforms the original value to a literal.
      * @return
      *   a literal with the same type.
      */
    def lit: DoricColumn[L] = functions.lit(litv).pure[Doric].toDC
  }

}
