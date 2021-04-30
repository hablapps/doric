package habla.doric

trait UnsafeCasting[From, To] {
  def cast(column: DoricColumn[From])(implicit
      constructor: FromDf[To]
  ): DoricColumn[To]
}

object UnsafeCasting {
  @inline def apply[From, To](implicit
      imp: UnsafeCasting[From, To]
  ): UnsafeCasting[From, To] =
    implicitly[UnsafeCasting[From, To]]
}
trait SparkUnsafeCasting[From, To] extends UnsafeCasting[From, To] {
  override def cast(column: DoricColumn[From])(implicit
      constructor: FromDf[To]
  ): DoricColumn[To] =
    column.elem.map(_.cast(constructor.dataType)).toDC
}
