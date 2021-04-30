package habla.doric

package object implicitConversions {
  implicit def literalConversion[L](litv: L): DoricColumn[L] = {
    litv.lit
  }
}
