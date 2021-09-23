package doric
package types

trait NumericType[T]

object NumericType {
  implicit val intNumeric: NumericType[Int]       = new NumericType[Int] {}
  implicit val longNumeric: NumericType[Long]     = new NumericType[Long] {}
  implicit val floatNumeric: NumericType[Float]   = new NumericType[Float] {}
  implicit val doubleNumeric: NumericType[Double] = new NumericType[Double] {}
}
