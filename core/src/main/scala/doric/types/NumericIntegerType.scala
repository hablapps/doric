package doric
package types

trait NumericIntegerType[T]

object NumericIntegerType {
  implicit val intNumeric: NumericType[Int]   = NumericType[Int]
  implicit val longNumeric: NumericType[Long] = NumericType[Long]
}
