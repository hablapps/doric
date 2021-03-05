package mrpowers.bebe

trait Casting[From, To] {
  def cast(column: From)(implicit constructor: FromDf[To], extractor: ToColumn[From]): To
}

trait SparkCasting[From, To] extends Casting[From, To] {
  override def cast(column: From)(implicit constructor: FromDf[To], extractor: ToColumn[From]): To =
    construct(column.sparkColumn.cast(constructor.dataType))
}
