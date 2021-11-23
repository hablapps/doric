package doric
package types

trait NumericDecimalsType[T]

object NumericDecimalsType {
  implicit val floatNumeric: NumericType[Float]   = NumericType[Float]
  implicit val doubleNumeric: NumericType[Double] = NumericType[Double]
}
