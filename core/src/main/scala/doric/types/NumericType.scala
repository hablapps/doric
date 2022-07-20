package doric
package types

trait NumericType[T] {
  type Sum
}

object NumericType {
  implicit val intNumeric: NumericType[Int] {
    type Sum = Long
  } = new NumericType[Int] {
    type Sum = Long
  }
  implicit val longNumeric: NumericType[Long] {
    type Sum = Long
  } = new NumericType[Long] {
    type Sum = Long
  }
  implicit val floatNumeric: NumericType[Float] {
    type Sum = Double
  } = new NumericType[Float] {
    type Sum = Double
  }
  implicit val doubleNumeric: NumericType[Double] {
    type Sum = Double
  } = new NumericType[Double] {
    type Sum = Double
  }
  implicit val shortNumeric: NumericType[Short] {
    type Sum = Short
  } = new NumericType[Short] {
    type Sum = Short
  }
}
