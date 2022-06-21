package doric
package syntax

trait NumWithDecimalsType[T]

object NumWithDecimalsType {
  def apply[T]: NumWithDecimalsType[T] = new NumWithDecimalsType[T] {}

  implicit val decimalsDouble: NumWithDecimalsType[Double] = NumWithDecimalsType[Double]

  implicit val decimalsFloat: NumWithDecimalsType[Float] = NumWithDecimalsType[Float]
}
