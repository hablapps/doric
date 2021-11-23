package doric
package types

trait NumericType[T] extends NumericDecimalsType[T]

object NumericType {
  def apply[T]: NumericType[T] = new NumericType[T] {}

  implicit val intNumeric: NumericType[Int]   = NumericType[Int]
  implicit val longNumeric: NumericType[Long] = NumericType[Long]
}
