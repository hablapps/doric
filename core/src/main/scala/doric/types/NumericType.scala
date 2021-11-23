package doric
package types

trait NumericType[T] extends NumericDecimalsType[T]

object NumericType {
  implicit val intNumeric: NumericType[Int]   = new NumericType[Int] {}
  implicit val longNumeric: NumericType[Long] = new NumericType[Long] {}
}
