package habla.doric

import cats.implicits.catsSyntaxApplicativeId

import org.apache.spark.sql.functions.lit

package object implicitConversions {
  implicit def literalConversion[L](litv: L): DoricColumn[L] = {
    lit(litv).pure[Doric].toDC
  }
}
