package habla.doric
package syntax

import cats.implicits.catsSyntaxApplicativeId

import org.apache.spark.sql.functions

trait LiteralConversions {

  implicit class LiteralOps[L](litv: L) {
    def lit: DoricColumn[L] = functions.lit(litv).pure[Doric].toDC
  }

}
