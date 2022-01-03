package doric
package syntax

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
  def lit[L: SparkType: LiteralSparkType](litv: L): LiteralDoricColumn[L] = {
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
    def lit: LiteralDoricColumn[L] = LiteralDoricColumn(litv)
  }

}
