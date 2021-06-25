package doric
package types

/**
  * Typeclass for castings that can be transformed to null
  *
  * @tparam From The origin type of the data
  * @tparam To The destiny type of the data
  */
trait UnsafeCasting[From, To] {
  def cast(column: DoricColumn[From])(implicit
      constructor: SparkType[To]
  ): DoricColumn[To]
}

private[doric] object UnsafeCasting {
  @inline def apply[From, To](implicit
      imp: UnsafeCasting[From, To]
  ): UnsafeCasting[From, To] =
    implicitly[UnsafeCasting[From, To]]

  implicit val stringCastToInt: UnsafeCasting[String, Int] =
    SparkUnsafeCasting[String, Int]

  implicit val stringCastToLong: UnsafeCasting[String, Long] =
    SparkUnsafeCasting[String, Long]

  implicit val stringCastToFloat: UnsafeCasting[String, Float] =
    SparkUnsafeCasting[String, Float]

  implicit val stringCastToDouble: UnsafeCasting[String, Double] =
    SparkUnsafeCasting[String, Double]

  implicit val longCastTInt: UnsafeCasting[Long, Int] =
    SparkUnsafeCasting[Long, Int]

  implicit val floatCastToInt: UnsafeCasting[Float, Int] =
    SparkUnsafeCasting[Float, Int]

  implicit val floatCastToLong: UnsafeCasting[Float, Long] =
    SparkUnsafeCasting[Float, Long]

  implicit val doubleCastToInt: UnsafeCasting[Double, Int] =
    SparkUnsafeCasting[Double, Int]

  implicit val doubleCastToLong: UnsafeCasting[Double, Long] =
    SparkUnsafeCasting[Double, Long]

  implicit val doubleCastToFloat: UnsafeCasting[Double, Float] =
    SparkUnsafeCasting[Double, Float]
}

/**
  * Utility to cast a value using spark `cast` method
  * @tparam From The origin type of the data
  * @tparam To The destiny type of the data
  */
trait SparkUnsafeCasting[From, To] extends UnsafeCasting[From, To] {
  override def cast(column: DoricColumn[From])(implicit
      constructor: SparkType[To]
  ): DoricColumn[To] =
    column.elem.map(_.cast(constructor.dataType)).toDC
}

object SparkUnsafeCasting {
  def apply[From, To]: UnsafeCasting[From, To] =
    new SparkUnsafeCasting[From, To] {}
}
