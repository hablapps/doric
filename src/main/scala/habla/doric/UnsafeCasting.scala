package habla.doric

/**
  * Typeclass for castings that can be transformed to null
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
