package habla.doric
package syntax

import habla.doric.{BooleanColumn, IntegerColumn, StringColumn}
import habla.doric.Literal

trait LiteralConversions {

  implicit class LiteralOps[L](lit: L) {
    def lit[O](implicit litTc: Literal[O, L]): DoricColumn[O] = Literal[O, L].createTLiteral(lit)
  }

}
