package habla.doric.syntax

import habla.doric.{BooleanColumn, IntegerColumn, StringColumn}
import habla.doric.Literal
import habla.doric.FromDf

trait LiteralConversions {

  implicit class LiteralOps[L](lit: L){
    def lit[O: FromDf](implicit litTc: Literal[O, L]): O = litTc.createTLiteral(lit)
  }

}
