package habla.doric

trait Casting[From, To] {
  def cast(column: DoricColumn[From])(implicit constructor: FromDf[To]): DoricColumn[To]
}

object Casting {
  @inline def apply[From, To](implicit imp: Casting[From, To]): Casting[From, To] =
   implicitly[Casting[From, To]]
}

trait SparkCasting[From, To] extends Casting[From, To] {
  override def cast(column: DoricColumn[From])(implicit constructor: FromDf[To]): DoricColumn[To] =
    DoricColumn(column.col.cast(constructor.dataType))
}
