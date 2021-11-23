package doric
package types

trait NumericDecimalsType[T]

object NumericDecimalsType {
  implicit val floatNumeric: NumericType[Float]   = new NumericType[Float] {}
  implicit val doubleNumeric: NumericType[Double] = new NumericType[Double] {}
}
