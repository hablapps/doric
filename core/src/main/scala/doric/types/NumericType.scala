package doric
package types

trait NumericType[T] extends NumericIntegerType[T] with NumericDecimalsType[T]

object NumericType {
  def apply[T]: NumericType[T] = new NumericType[T] {}
}
