package doric
package syntax

trait IntegralType[T]

object IntegralType {
  def apply[T]: IntegralType[T] = new IntegralType[T] {}

  implicit val integralInt: IntegralType[Int] = IntegralType[Int]

  implicit val integralLong: IntegralType[Long] = IntegralType[Long]
}
